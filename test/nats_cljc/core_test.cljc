(ns nats-cljc.core-test
  (:require [clojure.test :refer [deftest is]]
            [nats-cljc.core :as nats]))

(deftest scaffold-compiles-and-runs
  ;; Trivial assertion that exercises the cross-platform test harness on every
  ;; platform (JVM / Node / browser). Replaced by real coverage in later slices.
  (is (string? nats/version)))
