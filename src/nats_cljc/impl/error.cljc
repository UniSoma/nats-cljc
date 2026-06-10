(ns ^:no-doc nats-cljc.impl.error
  "Shared, platform-neutral error classification (ADR 0006). The per-leg
   ErrorListener / status plumbing stays quarantined in `impl.jvm` / `impl.js`
   (ADR 0005); only the pure message → `:type` classifier is portable, so it lives
   here — the one-fn shared seam matching the `codec/->codec-error` precedent. Both
   legs feed it their native server-error string (jnats' ErrorListener error /
   nats.js' status error message), so a single fn — and a single cross-leg test —
   covers both."
  (:require [clojure.string :as str]))

(defn server-error-type
  "Classify a server-error message string onto a connection-level error `:type`
   (ADR 0006): the exact \"Permissions Violation\" → `:permissions-violation`,
   anything else (including an absent string) → `:protocol-error`. No clean e2e
   trigger exists for the latter, so it is exercised as a classifier unit shared by
   both legs."
  [error]
  (if (and error (str/includes? error "Permissions Violation"))
    :permissions-violation
    :protocol-error))
