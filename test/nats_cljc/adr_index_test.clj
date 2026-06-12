(ns nats-cljc.adr-index-test
  "Guards docs/adr/README.md against drift. The cljdoc sidebar publishes only
   that index (not the individual ADRs), so an ADR missing from it is invisible
   to cljdoc readers — and this drift has already happened once (the old
   per-ADR sidebar went stale at 0015 while ADRs reached 0026). Plain `.clj` —
   it walks the filesystem, which has no browser/Node analogue, so shadow-cljs
   never loads it."
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]))

(deftest every-adr-is-in-the-index
  (let [index (slurp "docs/adr/README.md")
        adrs  (->> (.listFiles (io/file "docs/adr"))
                   (map #(.getName ^java.io.File %))
                   (filter #(re-matches #"\d{4}-.*\.md" %))
                   sort)]
    (is (seq adrs) "docs/adr/ must contain numbered ADR files")
    (doseq [adr adrs]
      (is (.contains ^String index adr)
          (str adr " is missing from docs/adr/README.md")))))
