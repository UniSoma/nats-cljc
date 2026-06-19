FROM aishell:foundation

SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

# >>> clojure (managed) >>>
# JDK 25 from Eclipse Temurin (foundation only ships JRE 17 headless).
COPY --from=eclipse-temurin:25-jdk-jammy /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

ARG CLOJURE_VERSION=1.12.5.1645
RUN curl -fsSL "https://github.com/clojure/brew-install/releases/download/${CLOJURE_VERSION}/linux-install.sh" -o /tmp/linux-install.sh; \
    bash /tmp/linux-install.sh; \
    rm /tmp/linux-install.sh; \
    clojure --version

ARG CLOJURE_LSP_VERSION=2026.05.05-12.58.26
RUN curl -fsSL "https://github.com/clojure-lsp/clojure-lsp/releases/download/${CLOJURE_LSP_VERSION}/clojure-lsp-native-static-linux-amd64.zip" -o /tmp/clojure-lsp.zip; \
    unzip /tmp/clojure-lsp.zip -d /usr/local/bin; \
    rm /tmp/clojure-lsp.zip; \
    clojure-lsp --version

ARG CLJ_KONDO_VERSION=2026.05.25
RUN curl -fsSL "https://github.com/clj-kondo/clj-kondo/releases/download/v${CLJ_KONDO_VERSION}/clj-kondo-${CLJ_KONDO_VERSION}-linux-static-amd64.zip" -o /tmp/clj-kondo.zip; \
    unzip /tmp/clj-kondo.zip -d /usr/local/bin; \
    rm /tmp/clj-kondo.zip; \
    clj-kondo --version

# Clojure MCP
RUN clojure -Ttools install-latest :lib io.github.bhauman/clojure-mcp :as mcp

ARG CLOJURE_MCP_LIGHT_VERSION=0.2.2
RUN bbin install https://github.com/bhauman/clojure-mcp-light.git --tag "v${CLOJURE_MCP_LIGHT_VERSION}"; \
    bbin install https://github.com/bhauman/clojure-mcp-light.git --tag "v${CLOJURE_MCP_LIGHT_VERSION}" --as clj-nrepl-eval --main-opts '["-m" "clojure-mcp-light.nrepl-eval"]'; \
    bbin install https://github.com/bhauman/clojure-mcp-light.git --tag "v${CLOJURE_MCP_LIGHT_VERSION}" --as clj-paren-repair --main-opts '["-m" "clojure-mcp-light.paren-repair"]'

ARG CLJ_SURGEON_SHA=6baa2a4dddc32f8cf5c7f86106beb0ed13c1deb0
RUN bbin install https://github.com/realgenekim/clj-surgeon.git --git/sha "${CLJ_SURGEON_SHA}"
# <<< clojure (managed) <<<

# nats-server (ws:// listener) for local integration tests — pinned to CI's version.
ARG NATS_SERVER_VERSION=2.14.1
RUN curl -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_SERVER_VERSION}/nats-server-v${NATS_SERVER_VERSION}-linux-amd64.tar.gz" | tar -xz -C /tmp; \
    mv "/tmp/nats-server-v${NATS_SERVER_VERSION}-linux-amd64/nats-server" /usr/local/bin/; \
    rm -rf "/tmp/nats-server-v${NATS_SERVER_VERSION}-linux-amd64"; \
    nats-server --version

# nats CLI for poking the server during development/debugging.
ARG NATS_CLI_VERSION=0.4.0
RUN curl -fsSL "https://github.com/nats-io/natscli/releases/download/v${NATS_CLI_VERSION}/nats-${NATS_CLI_VERSION}-linux-amd64.zip" -o /tmp/nats-cli.zip; \
    unzip -q /tmp/nats-cli.zip -d /tmp; \
    mv "/tmp/nats-${NATS_CLI_VERSION}-linux-amd64/nats" /usr/local/bin/; \
    rm -rf /tmp/nats-cli.zip "/tmp/nats-${NATS_CLI_VERSION}-linux-amd64"; \
    nats --version

# nsc generates the nkey / JWT / creds fixtures for the advanced-auth test legs
# (one-time, at fixture-authoring time — CI only starts nats-server against the
# checked-in ci/nats-*.conf, so it needs nsc nowhere).
ARG NSC_VERSION=2.14.0
RUN curl -fsSL "https://github.com/nats-io/nsc/releases/download/v${NSC_VERSION}/nsc-linux-amd64.zip" -o /tmp/nsc.zip; \
    unzip -q /tmp/nsc.zip -d /tmp/nsc; \
    mv /tmp/nsc/nsc /usr/local/bin/; \
    rm -rf /tmp/nsc.zip /tmp/nsc; \
    nsc --version

# >>> knot (managed) >>>
ARG KNOT_VERSION=0.7.0
RUN bbin install https://github.com/UniSoma/knot.git --tag "v${KNOT_VERSION}"
# <<< knot (managed) <<<
