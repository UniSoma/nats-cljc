(ns nats-cljc.auth
  "Shared, platform-neutral `:auth` classification (ADR 0005). The per-leg
   credential wiring stays quarantined in `impl.jvm` / `impl.js` — each `with-auth`
   `case` dispatches native interop (jnats' Options builder / nats.js' options map)
   that can't be portable — but the pure `{:auth ...}` map → variant tag is, so it
   lives here, the one-fn shared seam matching the `error/server-error-type` and
   `codec/->codec-error` precedents. Both legs' `case` branch on this tag, so a
   single fn — and a single cross-leg test — pins which credentials an `:auth` map
   selects on JVM and JS alike, where two verbatim copies could silently drift.")

(defn auth-variant
  "Derive the tagged `:auth` variant — exactly one of :token / :user-pass / :nkey /
   :jwt / :creds, or nil when no auth is configured. The shapes are mutually
   exclusive, so each is keyed off its own discriminating field; :seed is therefore
   read by exactly one variant (:jwt when a jwt is present, else :nkey) rather than
   shared across two. A stray field beside another shape can no longer silently
   switch methods."
  [{:keys [token user nkey jwt creds]}]
  (cond
    token :token
    user  :user-pass
    jwt   :jwt
    nkey  :nkey
    creds :creds))
