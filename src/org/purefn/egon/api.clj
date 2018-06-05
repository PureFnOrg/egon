(ns org.purefn.egon.api
  (:require [clojure.spec.alpha :as s]
            [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
            [org.purefn.egon.protocol :as proto]))

(defn list-keys
  "Lists all of the keys in the specified s3 bucket."
  [s3 bucket] 
  (proto/list-keys s3 bucket))

(defn bucket-exists?
  "Determines if the specified bucket exists.

  Returns true if found, false if not."
  [s3 bucket]
  (proto/bucket-exists? s3 bucket))

(defn store
  "Stores a document in a s3 bucket.  Accepts the following optional arguments:
  :meta - a map of meta-data to store along with the document."
  [s3 bucket key value & {:keys [meta]}]
  (proto/store s3 bucket key value meta))

(defn fetch
  "Fetches a document from an s3 bucket.  Accepts an optional sequence of keywords:
  :meta - indicates whether to return metadata long with the document content.
          When supplied, and map is returned with the the document as the :content
          key and metadata as other keys in the map."
  [s3 bucket key & opts]
  (proto/fetch s3 bucket key (set opts)))

(defn destroy
  "Deletes a document from an s3 bucket."
  [s3 bucket key]
  (proto/destroy s3 bucket key))

(defn store*
  "Attempts to store a document in a s3 bucket.  Accepts the following optional arguments:
  :meta - a map of meta-data to store along with the document.

  Returns an AWS `PutObjectResult` wrapped in a `Success` object."
  [s3 bucket key value & {:keys [meta]}]
  (proto/store* s3 bucket key value meta))

(defn fetch*
  "Fetches a document from an s3 bucket.  Accepts an optional sequence of keywords:
  :meta - indicates whether to return metadata long with the document content.
          When supplied, and map is returned with the the document as the :content
          key and metadata as other keys in the map.

  Returns the s3 object wrapped in a `Success` object if successful, `Failure` if not."
  [s3 bucket key & opts]
  (proto/fetch* s3 bucket key (set opts)))

(defn destroy*
  "Deletes a document from an s3 bucket.
  
  Returns an AWS `DeleteObjectResult` wrapped in a `Success` object if successful, `Failure` if not."
  [s3 bucket key]
  (proto/destroy* s3 bucket key))

;;------------------------------------------------------------------------------
;; Specs
;;------------------------------------------------------------------------------
(s/def ::reason #{::fatal ::doc-missing ::bucket-missing
                  ::auth-failed ::server-unreachable})

(s/def ::storeable-type #{java.io.InputStream
                          (Class/forName "[B")
                          String
                          clojure.lang.IPersistentMap})

(def s3-record? (partial satisfies? proto/AwsS3))

;; included only special characters which are generally safe according to the s3 docs
;; http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
(def key?
  (s/spec (s/and string? (partial re-matches #"^[0-9a-zA-Z/!\-_\.\*'()]+$"))
          :gen #(string-from-regex #"[0-9a-zA-Z/!\-_\.\*'()]+")))

(s/def ::key key?)

(s/fdef list-keys
        :args (s/cat :s3 s3-record?
                     :bucket string?))

(s/fdef bucket-exists?
        :args (s/cat :s3 s3-record?
                     :bucket string?)
        :ret boolean?)

(s/fdef store
        :args (s/cat :s3 s3-record?
                     :key key?
                     :value ::storeable-type))

(s/fdef fetch
        :args (s/cat :s3 s3-record?
                     :bucket string?
                     :key key?)
        :ret ::storeable-type)

(s/fdef destroy
        :args (s/cat :s3 s3-record?
                     :bucket string?
                     :key key?))
