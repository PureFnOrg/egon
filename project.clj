(defproject org.purefn/egon "0.2.3-SNAPSHOT"
  :description "Interface to Amazon S3"
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [com.taoensso/timbre "4.7.4"]
                 [org.purefn/kurosawa "1.10.7"]
                 [com.amazonaws/aws-java-sdk-core "1.11.142"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.142"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component "0.3.2"]]
                   :source-paths ["dev"]}})
