(ns dev
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.stuartsierra.component :as component]
   [org.purefn.egon.core :as egon]
   [org.purefn.kurosawa.k8s :as k8s]
   [org.purefn.kurosawa.config :as config]
   [org.purefn.egon.api :as api]
   [org.purefn.kurosawa.log.core :as klog]
   [taoensso.timbre :as log]))

(clojure.tools.namespace.repl/set-refresh-dirs "dev" "src" "test")

(defn default-system
  []
  (component/system-map
   :s3 (egon/s3 (merge (egon/default-config)
                       {:buckets ["test"]}))))

(def system nil)

(defn init
  "Constructs the current development system."
  []
  (alter-var-root #'system
                  (constantly (default-system))))

(defn start
  "Starts the current development system."
  []
  (alter-var-root #'system component/start))

(defn stop
  "Shuts down and destroys the current development system."
  []
  (alter-var-root #'system
                  (fn [s] (when s (component/stop s)))))

(defn go
  "Initializes and starts the system running."
  []
  (init)
  (klog/init-dev-logging system [])
  (start)
  :ready)

(defn reset
  "Stops the system, reloads modified source files, and restarts it."
  []
  (stop)
  (refresh :after `go))

(defn reset-logging! []
  (log/set-config! log/example-config))

(defn write-and-read-map []
  (let [s3 (:s3 system)
        m {:foo {:bar "baz"}}
        bucket "test"
        key "test-map"]
    (api/store s3 bucket key m)
    (api/fetch s3 bucket key)))

(defn write-and-read-map-with-meta []
  (let [s3 (:s3 system)
        m {:foo {:bar "baz"}}
        bucket "test"
        key "test-map"
        meta {:version "2017"}]
    (api/store s3 bucket key m :meta meta)
    (api/fetch s3 bucket key :meta)))

(defn write-and-read-versioned-object []
  (let [s3 (:s3 system)
        bucket "test"
        key "my-key"
        putObjectResult1 (api/store s3 bucket key {:v1 1})
        putObjectResult2  (api/store s3 bucket key {:v2 2})
        v1-id (.getVersionId putObjectResult1)
        v2-id (.getVersionId putObjectResult2)]
    {:v1 (api/fetch s3 bucket key :version-id v1-id :meta true)
     :v2 (api/fetch s3 bucket key :version-id v2-id :meta true)}))
