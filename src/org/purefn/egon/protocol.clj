(ns org.purefn.egon.protocol)

(defprotocol AwsS3
  (store [this bucket key value meta])
  (store* [this bucket key value meta])
  (fetch [this bucket key opts])
  (fetch* [this bucket key opts])
  (destroy [this bucket key])
  (destroy* [this bucket key])
  (bucket-exists? [this bucket])
  (list-keys [this bucket]))
