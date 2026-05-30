(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            #?(:cljs [promesa.core :as p])))

;; Transport differs per platform (ADR 0001): TCP on the JVM, WebSocket on CLJS.
;; Only the URL forks; the connect/publish/subscribe calls below are identical.
(def ^:private server-url
  #?(:clj  "nats://127.0.0.1:4222"
     :cljs "ws://127.0.0.1:8080"))

(def ^:private subject "tracer.roundtrip")
(def ^:private payload {:hello "world" :n 42 :nested [1 2 {:k :v}]})

;; Test-only teardown: close the native client so jnats' non-daemon threads (and
;; the CLJS ws connection) don't outlive the test. A public close/drain is its
;; own slice; here we reach the record's client field directly.
(defn- close! [conn] (.close (:client conn)))

(deftest connect-resolves-to-a-connection
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})]
       (try
         (is (some? conn) "connect resolves to a non-nil Connection")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (is (some? conn) "connect resolves to a non-nil Connection")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest publish-subscribe-round-trip
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)
           sub      (nats/subscribe conn subject #(deliver received %))
           pub-ret  (nats/publish conn subject payload)]
       (try
         (is (some? sub) "subscribe returns a Subscription synchronously")
         (is (nil? pub-ret) "publish returns nil")
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= subject (:subject msg)) "handler receives the subject")
           (is (= payload (:data msg)) "handler receives EDN-decoded :data"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)
                                sub      (nats/subscribe conn subject #(p/resolve! received %))
                                pub-ret  (nats/publish conn subject payload)]
                            (is (some? sub) "subscribe returns a Subscription synchronously")
                            (is (nil? pub-ret) "publish returns nil")
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= subject (:subject msg)) "handler receives the subject")
                                          (is (= payload (:data msg)) "handler receives EDN-decoded :data")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))
