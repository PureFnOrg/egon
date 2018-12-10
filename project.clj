(defproject org.purefn/egon "0.2.4"
  :description "Interface to Amazon S3"
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [com.taoensso/timbre "4.7.4"]
                 [org.purefn/kurosawa.core "2.0.6"]
                 [com.amazonaws/aws-java-sdk-core "1.11.462"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.462"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component "0.3.2"]]
                   :source-paths ["dev"]}})
