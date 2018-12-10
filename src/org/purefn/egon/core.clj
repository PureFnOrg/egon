(ns org.purefn.egon.core
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.spec.alpha :as spec]
   [com.stuartsierra.component :as component]
   [org.purefn.egon.api :as api]
   [org.purefn.egon.protocol :as proto]
   [org.purefn.egon.interop :as interop]
   [org.purefn.kurosawa.error :as error]
   [org.purefn.kurosawa.k8s :as k8s]
   [org.purefn.kurosawa.log.api :as log-api]
   [org.purefn.kurosawa.log.core :as klog]
   [org.purefn.kurosawa.log.protocol :as log-proto]
   [org.purefn.kurosawa.result :refer :all]
   [taoensso.timbre :as log])
  (:import
   (com.amazonaws SdkClientException)
   (com.amazonaws.auth AWSCredentials
                       BasicAWSCredentials
                       DefaultAWSCredentialsProviderChain)
   (com.amazonaws.services.s3 AmazonS3Client)
   (com.amazonaws.services.s3.model AmazonS3Exception)))

;;--------------------------------------------------------------------
;; Error handing
;;--------------------------------------------------------------------

(defn- reason
  [ex]
  (cond
    (instance? clojure.lang.ExceptionInfo ex) (::api/reason (ex-data ex) ::api/fatal)

    (and (instance? AmazonS3Exception ex)
         (str/includes? (.getMessage ex) "Status Code: 404")) ::api/doc-missing

    (and (instance? AmazonS3Exception ex)
         (str/includes? (.getMessage ex) "Status Code: 403")) ::api/auth-failed

    (and (instance? SdkClientException ex)
         (or (instance? java.net.UnknownHostException (.getCause ex))
             (instance? org.apache.http.conn.HttpHostConnectException (.getCause ex))))
    ::api/server-unreachable

    :default ::api/fatal))

(def ^:private snafu
  (partial error/snafu reason ::api/reason ::api/fatal))

;;--------------------------------------------------------------------
;; S3 helpers
;;--------------------------------------------------------------------

(defn- s3-client [{:keys [access-key secret-key]}]
  (AmazonS3Client. (BasicAWSCredentials. access-key secret-key)))

(defn- create-bucket
  "Create a new S3 bucket with the supplied name."
  [^AmazonS3Client client name]
  (.createBucket client name))

(defn- put-object
  [request ^AmazonS3Client client]
  (.putObject client request))

(defn- get-object
  [bucket key ^AmazonS3Client client]
  (.getObject client bucket key))

(defn- delete-object
  [bucket key ^AmazonS3Client client]
  (.deleteObject client bucket key))

(defn- list-objects
  [request ^AmazonS3Client client]
  (.listObjects client request))

(defn- bucket-exists? [bucket ^AmazonS3Client client]
  (.doesBucketExist client bucket))

(defn- bucket-name [{{:keys [bucket-suffix buckets]} :config} name]
  (if (some #{name} buckets)
    (succeed (str name bucket-suffix))
    (fail (ex-info "Unable to locate S3 bucket!"
                   {:bucket name
                    ::api/reason ::api/bucket-missing}))))

(defn- retry
  [{{:keys [max-retries initial-delay-ms unreachable-delay-ms]} :config} f]
  (error/retry-generic reason ::api/reason initial-delay-ms max-retries f
                       {::api/server-unreachable (partial + unreachable-delay-ms)}))

(defn- read-object [{{:keys [content-type]} :metadata :as obj}]
  (update obj :content
          (case content-type
            "application/edn" (comp edn/read-string slurp)
            "application/octet-stream" identity
            "plain/text" slurp)))

;;--------------------------------------------------------------------
;; Component
;;--------------------------------------------------------------------

(defrecord S3Client
    [client config]

  log-proto/Logging
  (log-namespaces [_]
    ["org.apache.http.*" "com.amazonaws.internal.*" "org.purefn.egon.*"])

  (log-configure [this dir]
    (klog/add-component-appender :s3 (log-api/log-namespaces this)
                                 (str dir "/s3.log")))

  component/Lifecycle
  (start [this]
    (doseq [bucket (:buckets config)
            :let [bucket-name (success (bucket-name this bucket))]]
      (when (and bucket-name (not (proto/bucket-exists? this bucket)))
        (log/info "Creating S3 bucket" bucket-name)
        (retry this
               #(-> (attempt create-bucket (client) bucket-name)
                    (recover (snafu "Unable to create S3 bucket!"
                                    {:bucket bucket}))))))
    this)

  (stop [_])

  proto/AwsS3
  (bucket-exists? [this bucket]
    (-> (retry this
               #(-> (bucket-name this bucket)
                    (proceed bucket-exists? (client))
                    (recover (snafu "Unable to determine if bucket exists!"
                                    {:bucket bucket}))))
        (success)))

  (store* [this bucket key value meta]
    (log/info "Storing to :key" key "in :bucket" bucket)
    (-> (bucket-name this bucket)
        (proceed interop/put-request key value meta)
        (proceed put-object (client))
        (recover (snafu "Failed to store document in S3!"
                        {:bucket bucket
                         :key key
                         :value value}))))

  (store [this bucket key value meta]
    (-> (retry this #(proto/store* this bucket key value meta))
        (success)))
  
  (fetch* [this bucket key opts]
    (log/info "Fetching :key" key "in :bucket" bucket)
    (let [xform (if (contains? opts :meta) identity (partial :content))]
      (-> (bucket-name this bucket)
          (proceed get-object key (client))
          (proceed interop/to-map)
          (proceed read-object)
          (proceed xform)
          (recover (snafu "Failed to fetch document from S3!"
                          {:bucket bucket
                           :key key})))))

  (fetch [this bucket key opts]
    (-> (retry this #(proto/fetch* this bucket key opts))
        (success)))

  (destroy* [this bucket key]
    (log/info "Deleting :key" key "from :bucket" bucket)
    (-> (bucket-name this bucket)
        (proceed delete-object key (client))
        (recover (snafu "Failed to delete document from S3!"
                        {:bucket bucket
                         :key key}))))

  (destroy [this bucket key]
    (-> (retry this #(proto/destroy* this bucket key))
        (success)))

  (list-keys [this bucket]
    (retry this
           #(-> (bucket-name this bucket)
                (proceed interop/list-request)
                (proceed list-objects (client))
                (proceed interop/to-map)
                (recover (snafu "Failed to list objects in S3 bucket!"
                                {:bucket bucket}))
                (success)))))

;;--------------------------------------------------------------------
;; Configuration
;;--------------------------------------------------------------------

(defn environment-credentials
  "Attempts to extract AWS credentials from the environment, using the
   AWS SDK default provider chain. Returns a map of :access-key
   and :secret-key or nil if no credentials are located."
  []
  (let [provider (DefaultAWSCredentialsProviderChain/getInstance)]
    (when-let [^AWSCredentials creds (try (.getCredentials provider)
                                          (catch Exception _ nil))]
      {:access-key (.getAWSAccessKeyId creds)
       :secret-key (.getAWSSecretKey creds)})))

(defn default-config
  ([name]
   (let [k8-config
         (when (k8s/kubernetes?)
           {:cred {:access-key (-> (k8s/secrets name)
                                   (get "access-key"))
                   :secret-key (-> (k8s/secrets name)
                                   (get "secret-key"))}})]
     (merge {:bucket-suffix ".qa.purefn.org"
             :initial-delay-ms 5
             :unreachable-delay-ms 500
             :max-retries 5}
            k8-config)))
  ([] (default-config "s3")))

(spec/def ::access-key string?)
(spec/def ::secret-key string?)
(spec/def ::creds (spec/keys :req-un [::access-key ::secret-key]))
(spec/def ::bucket-sufix string?)
(spec/def ::buckets (spec/coll-of string?))
(spec/def ::initial-delay-ms pos-int?)
(spec/def ::unreachable-delay-ms pos-int?)
(spec/def ::max-retries pos-int?)
(spec/def ::config (spec/keys :req-un [::creds ::bucket-suffix ::buckets
                                       ::initial-delay-ms ::unreachable-delay-ms ::max-retries]))

;;--------------------------------------------------------------------
;; Construction
;;--------------------------------------------------------------------

(defn s3
   "Returns a new S3Client component for config, a map of:

    * :creds                 API credentials, a map with :access-key
                             and :secret-key. Note this is will be
                             ignored when running in k8s in favor of
                             the k8s secrets.
    * :bucket-suffix         String that will be added to each bucket
                             name, intended to assist with environment
                             separation. It is transparently added on
                             key reads and writes, so clients should
                             only refer to the unsuffixed bucket name.
    * :buckets               A collection of string bucket names used
                             by the component. Each will be created on
                             startup if they don't exist.
    * :initial-delay-ms      Integer number of milliseconds to wait
                             for the first retry of failures (other
                             than unreachable network failures).
    * :unreachable-delay-ms  Integer number of milliseconds to wait
                             for retry of unreachable network failures.
    * :max-retries           Integer max number of retries of failure."
  ([{:keys [cred] :as config}]
   (spec/assert ::config config)
   (->S3Client #(s3-client cred) (dissoc config :cred))))
