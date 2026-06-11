(ns build
  "tools.build script (ADR 0009). `clj -T:build jar` produces the library jar with
   SCM, a description, and an Apache-2.0 license entry in the pom; `clj -T:build
   deploy` pushes that jar to Clojars. deps-deploy 0.2.5 dropped its maven-core-only
   class reference, so it co-exists with tools.build on one classpath (the `:build`
   alias) — no separate deploy process needed."
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'io.github.unisoma/nats-cljc)
(def version "0.5.0")
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
                ;; cljdoc fetches articles from git at this exact tag, so it must
                ;; match the release tag (`v<version>`), never HEAD (ADR 0009).
                :scm       {:url                 "https://github.com/unisoma/nats-cljc"
                            :connection          "scm:git:git://github.com/unisoma/nats-cljc.git"
                            :developerConnection "scm:git:ssh://git@github.com/unisoma/nats-cljc.git"
                            :tag                 (str "v" version)}
                :pom-data  [[:description "NATS for Clojure and ClojureScript under one portable .cljc API."]
                            [:url "https://github.com/unisoma/nats-cljc"]
                            [:licenses
                             [:license
                              [:name "Apache-2.0"]
                              [:url "https://www.apache.org/licenses/LICENSE-2.0.txt"]
                              [:distribution "repo"]]]]})
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file})
  (println "Wrote" jar-file))

;; clj -T:build deploy — push target/<jar> to Clojars (run `jar` first). Reads
;; CLOJARS_USERNAME / CLOJARS_PASSWORD (a deploy token) from the env. :pom-file is
;; required: deps-deploy 0.2.5 reads the coordinates from it and does NOT crack open
;; the jar, so point it at the pom `jar` wrote under class-dir. Unsigned — Clojars no
;; longer requires signing.
(defn deploy [_]
  (dd/deploy {:installer      :remote
              :artifact       jar-file
              :pom-file       (b/pom-path {:lib lib :class-dir class-dir})
              :sign-releases? false}))
