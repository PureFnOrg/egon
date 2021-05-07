(ns org.purefn.egon.interop
  (:require [clojure.walk :as walk])
  (:import
   [com.amazonaws.services.s3.model
    Bucket Owner ObjectMetadata S3Object PutObjectRequest
    ObjectListing ListObjectsRequest S3ObjectSummary
    GetObjectRequest]
   [java.io File InputStream ByteArrayInputStream]))

;; to-map

(defprotocol Mappable
  "Convert an AWS SDK object into a Clojure map."
  [to-map [m]])

(extend-protocol Mappable
  Owner
  (to-map [owner]
    {:id (.getId owner)
     :display-name (.getDisplayName owner)})

  Bucket
  (to-map [bucket]
    {:name (.getName bucket)
     :creation-date (.getCreationDate bucket)
     :owner (to-map (.getOwner bucket))})

  ObjectMetadata
  (to-map [metadata]
    (into {} (filter (comp some? val))
          {:cache-control (.getCacheControl metadata)
           :content-disposition (.getContentDisposition metadata)
           :content-encoding (.getContentEncoding metadata)
           :content-length (.getContentLength metadata)
           :content-md5 (.getContentMD5 metadata)
           :content-type (.getContentType metadata)
           :etag (.getETag metadata)
           :last-modified (.getLastModified metadata)
           :server-side-encryption (.getServerSideEncryption metadata)
           :user (walk/keywordize-keys (into {} (.getUserMetadata metadata)))
           :version-id (.getVersionId metadata)}))

  S3Object
  (to-map [object]
    {:content (.getObjectContent object)
     :metadata (to-map (.getObjectMetadata object))
     :bucket (.getBucketName object)
     :key (.getKey object)})

  ObjectListing
  (to-map [listing]
   {:bucket (.getBucketName listing)
    :objects (map to-map (.getObjectSummaries listing))
    :prefix (.getPrefix listing)
    :common-prefixes (seq (.getCommonPrefixes listing))
    :truncated? (.isTruncated listing)
    :max-keys (.getMaxKeys listing)
    :marker (.getMarker listing)
    :next-marker (.getNextMarker listing)})

  S3ObjectSummary
  (to-map [summary]
    {:metadata {:content-length (.getSize summary)
                :etag (.getETag summary)
                :last-modified (.getLastModified summary)}
     ;;:bucket (.getBucketName summary)
     :key (.getKey summary)}))

;; put-request

(defprotocol Storeable
  "Returns a tuple of [`InputStream` `ObjectMetaData`] for the given object 
  whose type is storeable in S3."
  (format-data [s]))

(extend-protocol Storeable
  (Class/forName "[B")
  (format-data [bytes]
    [(ByteArrayInputStream. bytes)
     (doto (ObjectMetadata.)
       (.setContentType "application/octet-stream")
       (.setContentLength (alength bytes)))])
  
  InputStream
  (format-data [is]
    [is (doto (ObjectMetadata.)
          (.setContentType "application/octet-stream"))])

  String
  (format-data [s]
    (let [bytes (. s getBytes)]
      [(ByteArrayInputStream. bytes)
       (doto (ObjectMetadata.)
         (.setContentType "text/plain")
         (.setContentLength (alength bytes)))]))

  clojure.lang.IPersistentMap
  (format-data [m]
    (let [bytes (-> m pr-str .getBytes)]
      [(ByteArrayInputStream. bytes)
       (doto (ObjectMetadata.)
         (.setContentType "application/edn")
         (.setContentLength (alength bytes)))])))

(defn get-request [bucket key version-id]
  (if version-id
    (GetObjectRequest. bucket key version-id)
    (GetObjectRequest. bucket key)))

(defn put-request [bucket key data meta-data]
  (let [[in meta] (format-data data)]
    (when meta-data
      (.setUserMetadata meta (walk/stringify-keys meta-data)))
    (PutObjectRequest. bucket key in meta)))

;; list-request

(defn list-request [bucket]
  (doto (ListObjectsRequest.)
    (.setBucketName bucket)))
