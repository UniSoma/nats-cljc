(ns nats-cljc.version-test
  "Guards against version drift. The public `nats-cljc.core/version` ships in the
   artifact; `build.clj` carries a matching constant for the jar/pom; CHANGELOG.md
   is what humans read. This pins the var to the latest CHANGELOG release heading so
   the three can never silently disagree at release time. Plain `.clj` — it slurps a
   file, which has no browser/Node analogue, so shadow-cljs never loads it."
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as str]
            [nats-cljc.core :as nats]))

(defn- latest-changelog-version
  "The version from the first `## [x.y.z] - …` heading in CHANGELOG.md, skipping
   the `## [Unreleased]` placeholder."
  []
  (->> (str/split-lines (slurp "CHANGELOG.md"))
       (keep #(second (re-matches #"## \[(\d+\.\d+\.\d+[^\]]*)\].*" %)))
       first))

(deftest version-matches-changelog
  (is (= nats/version (latest-changelog-version))
      "nats-cljc.core/version must match the latest CHANGELOG.md release heading"))
