(ns build
  "tools.build script. `clj -T:build jar` produces the library jar with an
   Apache-2.0 license entry in the pom (ADR 0009)."
  (:require [clojure.tools.build.api :as b]))

(def lib 'io.github.UniSoma/nats-cljc)
(def version "0.1.0-SNAPSHOT")
(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     @basis
                :src-dirs  ["src"]
                :pom-data  [[:licenses
                             [:license
                              [:name "Apache-2.0"]
                              [:url "https://www.apache.org/licenses/LICENSE-2.0.txt"]
                              [:distribution "repo"]]]]})
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file})
  (println "Wrote" jar-file))
