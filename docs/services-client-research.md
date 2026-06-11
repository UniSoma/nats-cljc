# Services abstraction in the wrapped clients (research)

Pre-implementation survey of how the NATS **services** (`micro`) framework is
implemented in the two libraries `nats-cljc` wraps. Source of truth: the actual
artifacts, not docs —

- **JVM**: `io.nats/jnats 2.25.3`, package `io.nats.service` (`javap` of the jar).
- **JS**: `@nats-io/services` (latest `3.4.0`, peer `@nats-io/nats-core@3.4.0`).
  **Not yet a dependency** — `package.json` pins the nats-io trio at `3.3.1` and
  ships no `services` package. Adding it bumps the floor to nats-core `3.4.0`.

Both are pure conveniences over core request-reply: a queue-subscribed handler
plus auto-responders on the `$SRV.PING|INFO|STATS.*` control subjects. No new
wire protocol, no server feature — a client calls an endpoint with an ordinary
request.

---

## 1. JVM — `io.nats.service`

Builder-centric, synchronous construction, explicit lifecycle.

### Service construction
```
Service.builder()                       // io.nats.service.ServiceBuilder
  .connection(Connection)
  .name(String) .version(String) .description(String)
  .metadata(Map<String,String>)
  .addServiceEndpoint(ServiceEndpoint)  // repeatable
  .drainTimeout(Duration|long)          // default 5000ms
  .pingDispatcher / infoDispatcher / statsDispatcher / schemaDispatcher(Dispatcher)
  .build();                             // -> Service
```
- `Service.startService()` → `CompletableFuture<Boolean>` (async start handle).
- `Service.addServiceEndpoints(ServiceEndpoint…)` adds after build too.
- Lifecycle: `stop()` / `stop(boolean drain)` / `stop(Throwable)` / `stop(drain, Throwable)`,
  `reset()` (zeroes stats), `isStarted(...)`.
- Introspection: `getId/getName/getVersion/getDescription`,
  `getPingResponse() / getInfoResponse() / getStatsResponse()`,
  `getEndpointStats(name)`.
- Constants: `SRV_PING`, `SRV_INFO`, `SRV_STATS`, `DEFAULT_SERVICE_PREFIX`.

### Endpoint model — three layers
1. **`Endpoint`** — the *declaration* (name, subject, queueGroup, metadata).
   Builder: `name / subject / queueGroup / noQueueGroup / metadata`.
   `DEFAULT_QGROUP` constant. JSON-serializable.
2. **`Group`** — subject-prefix namespace. `new Group("calc")`,
   `appendGroup(Group)` chains (`getNext()`), `getSubject()` is the dotted prefix.
3. **`ServiceEndpoint`** — binds an `Endpoint`/`Group` to a **handler**. Builder:
   ```
   ServiceEndpoint.builder()
     .group(Group) .endpoint(Endpoint)            // or endpointName/Subject/QueueGroup/Metadata
     .handler(ServiceMessageHandler)              // void onMessage(ServiceMessage)
     .dispatcher(Dispatcher)
     .statsDataSupplier(Supplier<JsonValue>)      // custom per-endpoint stats payload
     .build();
   ```

### Handler + message
- `ServiceMessageHandler` is a one-method SAM: `void onMessage(ServiceMessage)`.
- `ServiceMessage`: `getSubject/getReplyTo/getData/getHeaders/hasHeaders`, plus
  reply verbs that **take the `Connection`**:
  `respond(conn, byte[]|String|JsonSerializable [, Headers])`,
  `respondStandardError(conn, String message, int code)`.
  Error constants `NATS_SERVICE_ERROR`, `NATS_SERVICE_ERROR_CODE`.

### Discovery (client side)
`new Discovery(Connection [, maxTimeMillis, maxResults])` →
`ping() / info() / stats()`, each in three arities: all services
(`List<…Response>`), by name (`List`), by name+id (single). Inbox override via
`setInboxSupplier`.

### Response/stats types (all `JsonSerializable`, extend `ServiceResponse`)
`ServiceResponse`: `getType/getId/getName/getVersion/getMetadata`, `serialize()`.
- `PingResponse` — identity only.
- `InfoResponse` — `getDescription()`, `getEndpoints(): List<Endpoint>`.
- `StatsResponse` — `getStarted(): ZonedDateTime`, `getEndpointStatsList(): List<EndpointStats>`.
- `EndpointStats` — `getName/getSubject/getQueueGroup`, `getNumRequests/getNumErrors`,
  `getProcessingTime/getAverageProcessingTime` (nanos), `getLastError`,
  `getData()`/`getDataAsJson()`, `getStarted()`.

---

## 2. JS — `@nats-io/services`

Promise/async-iterator-centric. Entry is the factory `Svcm`.

```ts
import { Svcm } from "@nats-io/services";
const svc = new Svcm(nc);                 // wraps a NatsConnection
const service = await svc.add(config);    // ServiceConfig -> Promise<Service>
const client  = svc.client(opts?, prefix?); // -> ServiceClient (discovery)
```
(The README's `new Svc(nc)` is shorthand; the exported class is **`Svcm`**.)

### ServiceConfig
`name`, `version` (required); `description?`, `metadata?: Record<string,string>`,
`queue?` (default `"q"`), `statsHandler?: (Endpoint) => Promise<unknown|null>`.

### Groups & endpoints — `ServiceGroup` (Service extends it)
- `addGroup(subject?, queue?): ServiceGroup` — nestable.
- `addEndpoint(name, opts?: ServiceHandler | EndpointOptions): QueuedIterator<ServiceMsg>`.

Two handling styles, unlike the JVM's handler-only model:
1. **Callback**: `addEndpoint("max", (err, msg) => { msg?.respond(); })`.
2. **Async iterator** (the return value): `for await (const m of ep) m.respond(data)`.

`Endpoint`: `subject`, `handler?`, `metadata?`, `queue?` (override).

### Message — `ServiceMsg extends Msg`
Inherits core `Msg.respond(payload, opts?)`. Adds
`respondError(code: number, description: string, data?: Payload, opts?): boolean`.
Error helpers: `ServiceError extends Error` (`code`), statics
`isServiceError(msg)` / `toServiceError(msg)`; headers `ServiceErrorHeader`
(`"Nats-Service-Error"`), `ServiceErrorCodeHeader` (`"Nats-Service-Error-Code"`).

### Service lifecycle
`stopped: Promise<null|Error>`, `isStopped: boolean`, `stop(err?): Promise<null|Error>`,
`reset(): void`, plus introspection `info(): ServiceInfo`, `ping(): ServiceIdentity`,
`stats(): Promise<ServiceStats>`.

### Discovery — `ServiceClient`
`ping/info/stats(name?, id?)` each → `Promise<QueuedIterator<…>>` (iterate the
responses). Built via `svc.client(requestManyOpts?, prefix?)`.

### Wire/identity types (snake_case on the wire)
- `ServiceIdentity`: `type, name, id, version, metadata?`.
- `ServiceInfo extends ServiceIdentity`: `description`, `endpoints: EndpointInfo[]`
  (`name, subject, metadata?, queue_group?`).
- `NamedEndpointStats`: `name, subject, num_requests, num_errors, last_error?,
  data?, processing_time, average_processing_time` (Nanos), `queue_group?`.
- `EndpointStats extends ServiceIdentity`: `endpoints?: NamedEndpointStats[]`, `started` (ISO).
- `ServiceStats extends ServiceIdentity & EndpointStats`.

---

## 3. JVM ↔ JS map (what the `.cljc` facade has to reconcile)

| Concept              | JVM (`io.nats.service`)                           | JS (`@nats-io/services`)                          |
|----------------------|---------------------------------------------------|---------------------------------------------------|
| Entry                | `Service.builder()…build()`                       | `new Svcm(nc).add(config)` → `Promise<Service>`   |
| Construct style      | builder, sync `build()` + async `startService()`  | single async `add()`                              |
| Endpoint declaration | `Endpoint` + `Group` + `ServiceEndpoint` (3 types)| `addGroup` / `addEndpoint` on the service object   |
| Handler              | `ServiceMessageHandler.onMessage` (push only)     | callback **or** `QueuedIterator` (pull)           |
| Reply                | `msg.respond(conn, …)` (needs `Connection`)       | `msg.respond(…)` (msg carries the connection)     |
| Error reply          | `respondStandardError(conn, msg, code)`           | `respondError(code, desc, data?, opts?)`          |
| Discovery            | `new Discovery(conn)` → `List<…>`                 | `svc.client()` → `Promise<QueuedIterator<…>>`     |
| Stop                 | `stop(drain?, throwable?)` (void)                 | `stop(err?)` → `Promise<null\|Error>`             |
| Reset stats          | `reset()`                                         | `reset()`                                         |
| Default queue group  | `Endpoint.DEFAULT_QGROUP` (`"q"`)                 | `queue` default `"q"`                             |
| Stats field casing   | accessor methods (`getNumRequests`)               | snake_case wire fields (`num_requests`)           |

### Notes for the facade design
- **Async shape divergence is the headline.** JVM hands you a built object
  synchronously and a `CompletableFuture<Boolean>` to start; JS gives one
  `Promise<Service>`. The portable surface should present a single async
  "create+start" returning a platform-native promise (ADR 0002), as KV/JetStream do.
- **Reply ergonomics differ:** JVM `respond` needs the `Connection` threaded in;
  JS messages are self-contained. The wrapped `ServiceMessage` must capture the
  connection on the JVM leg so the portable `respond` takes only payload+headers.
- **Handler delivery:** JS supports a pull iterator; JVM is push-only. To stay
  portable, expose the push handler shape (matches ADR 0007) and not the iterator.
- **Stats normalization:** JVM exposes typed accessors, JS raw snake_case maps —
  the facade should normalize to one EDN shape (kebab keys), mirroring how
  KV/JetStream normalize their info/stats.
- **Dependency floor:** wiring JS services requires adding `@nats-io/services`
  and lifting the nats-io packages to `3.4.0` (peer requirement). This is a
  bundle-isolation concern (ADR 0016): services must not leak into core-only
  consumers — a new `:services` entry in the bundle/externs guards.
