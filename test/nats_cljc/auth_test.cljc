(ns nats-cljc.auth-test
  "Auth-variant classification (ADR 0005). `auth/auth-variant` is the pure
   `{:auth ...}` map → tag classifier both impl legs' `with-auth` `case` dispatches
   on; it needs no server, so these are plain `is` assertions on every platform.
   One shared test pins the classification — and the `:seed`-precedence ordering —
   so the JVM and JS legs can't drift on which credentials an `:auth` map selects."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])
            [nats-cljc.auth :as auth]))

(deftest auth-variant-classifies-each-shape
  (is (= :token (auth/auth-variant {:token "t"}))
      "a :token map classifies as :token")
  (is (= :user-pass (auth/auth-variant {:user "u" :pass "p"}))
      "a :user/:pass map classifies as :user-pass")
  (is (= :nkey (auth/auth-variant {:nkey "n" :seed "s"}))
      "an :nkey/:seed map classifies as :nkey")
  (is (= :jwt (auth/auth-variant {:jwt "j" :seed "s"}))
      "a :jwt/:seed map classifies as :jwt")
  (is (= :creds (auth/auth-variant {:creds "c"}))
      "a :creds map classifies as :creds")
  (is (nil? (auth/auth-variant {}))
      "an :auth map with no credential field classifies as nil")
  (is (nil? (auth/auth-variant nil))
      "an absent :auth classifies as nil"))

(deftest auth-variant-pins-the-cond-ordering
  ;; :seed is the one field two shapes (:jwt, :nkey) both read, so the precedence
  ;; between them is the rule that decides which credential a seed feeds. The cond
  ;; reads :jwt before :nkey, so a map carrying both — with a shared :seed — selects
  ;; :jwt; pin that here, since a drifted copy that reordered the two would silently
  ;; route the same map to nkey auth.
  (is (= :jwt (auth/auth-variant {:jwt "j" :nkey "n" :seed "s"}))
      ":jwt wins over :nkey when both are present, so :seed feeds the jwt path")
  ;; The remaining branches are mutually exclusive shapes, but pin the full cond
  ;; precedence so a stray field beside another shape can't silently switch methods.
  (is (= :token (auth/auth-variant {:token "t" :user "u" :jwt "j" :nkey "n" :creds "c"}))
      ":token outranks every other field")
  (is (= :user-pass (auth/auth-variant {:user "u" :jwt "j" :nkey "n" :creds "c"}))
      ":user-pass outranks :jwt / :nkey / :creds")
  (is (= :nkey (auth/auth-variant {:nkey "n" :creds "c"}))
      ":nkey outranks :creds"))
