# Feature - Egress data path: WebSocket, streaming, HTTP/2, and headers

## Overview

This design lets WebSockets, streamed responses, large bodies, and HTTP/2 pass through the egress gateway, and keeps every byte under policy. It adds no new action. The gateway asks `http:request` when the request head arrives, not after the whole body, and asks again while the exchange stays open only when a loaded rule needs to see more.

The model has five parts:
- **What an ask sees.** Facts about the head: the host, method, and path as today, plus `upgrade`, the header names, and `status` once the response head arrives. An ask sees no byte count.
- **What a record holds.** Each admitted ask gets one record, written at the next ask or at the close. It counts the body bytes that passed each way and the milliseconds since the previous record, so the records of an exchange add up to its totals and its lifetime. It also keeps `output.status`, as today.
- **When the gateway asks again.** After 1 MiB, if a budget sums that direction's bytes. At the response head, if a rule reads it. After 30 s, if a temporal rule covers `http:request`, such as a budget on time. And when an event could change a guard. With none of these, an exchange gets one ask and one record, as on main.
- **How rules use it.** A permit and a response check read the head. A guard, such as "nothing leaves after the SSH key is read", stops open exchanges before their next write. A budget sums the bytes or the time on the records. A rate limit counts one `::request` event for each exchange.
- **What a refusal does.** The workload gets 403 before the response head, the exchange is cut after it, and a WebSocket closes with 1008.

The cost is that an older `GET` permit now admits a WebSocket unless it adds `context.input.upgrade == ""`, no rule can cap the size or lifetime of one exchange, and a rate limit that passes its count also cuts the open exchanges to that host. Section 3 explains the model, section 4 shows what changes in a policy, and section 10 lists the seven PRs that deliver it.

## 1. Background

Today the egress gateway decides each HTTPS request once. It reads the whole request body, makes one `http:request` decision, sends the request, and reads the whole response. Then it adds the outcome to the history, the log of events that temporal rules read. That works for short requests, and it breaks for traffic that stays open or runs long:
- **WebSocket.** Realtime model APIs and browser automation keep a WebSocket open. Main (the main branch at `93a3761`) strips `Upgrade` and `Connection`, sends the handshake as a plain `GET`, and refuses a `101`.
- **Streaming.** A streamed model response reaches the workload only when it ends. Before the move to hyper, the first byte of a chunked response (10 chunks over 2 s) came after about 2,055 ms through the box and about 1 ms direct, as a median of 5. We measured that in a release build on macOS at `6418aad`, and main still reads whole bodies.
- **Size.** Bodies and responses over 16 MiB fail, so a large `git push` fails, and a 17 MiB response ended in a dropped connection at `e0af331`.
- **HTTP/2.** An HTTP/2-only client, such as a gRPC client, got the TLS alert `no_application_protocol`.
- **Headers.** An operator can't change a header limit or remove a header.

#230 was a prototype that relayed WebSockets and asked `http:request` again while one was open. Its review asked for a design first. We keep its relay, its measurements, and its idea of asking again, and we fix two problems. A `GET` permit admitted a WebSocket that uploads, with no warning. And each decision wrote a `::response` event, so a rate limit counted the decisions of an open WebSocket and could close it.

The idea of this design is to keep one action, `http:request`, and ask it again while a request stays open, but only when a loaded rule needs to see more.

## 2. How does a WebSocket pass through the gateway?

A WebSocket handshake is a `GET` with `Upgrade: websocket`, and the server answers `101 Switching Protocols`. After that, both sides send frames for as long as they like. The handshake takes the same path as any HTTPS request, and only step 4 is new:

```text
 workload              egress gateway                                    upstream
 1. CONNECT host:443   net:connect, then dial ----------------------------->
 2. TLS                ends TLS with a certificate from the box's CA
 3. GET + Upgrade      checks it, asks http:request, swaps in the secret -- GET -->
    <------ 101 ------ checks that the 101 answers this handshake <------ 101 --
 4. frames <---------> checks frame headers, asks again when needed <--> frames
```

The handshake follows these rules:
- The gateway accepts `wss://` only, because it reads messages only inside the TLS tunnel that it ends. A plain HTTP handshake gets 403 and a Deny record, the gateway's own record of a refusal, kept outside the history.
- The handshake must be a valid RFC 6455 `GET` on HTTP/1.1, or it gets 400. A handshake to a configured remote MCP (Model Context Protocol) server gets 403, because its frames would skip the `mcp:call` decision.
- The gateway swaps the placeholder token for the real credential on the handshake, never in a message. On a signing host, one whose requests the gateway signs with SigV4, it signs the handshake as a `GET` with no body.
- The gateway removes `Sec-WebSocket-Extensions` and refuses a `101` that names one, because `permessage-deflate` would hide a message from the scrub. The scrub masks each copy of a credential in what the server sends back.
- hyper's client takes any `101` as an upgrade, so the gateway checks `Upgrade`, `Connection`, and `Sec-WebSocket-Accept`, and answers 502 if one is wrong.

After the `101`, the relay copies frames between the two legs and checks each frame header against RFC 6455: reserved bits, opcodes, control frames of at most 125 bytes, masking by the workload only, and the order of a message's frames. It never holds a whole frame. It forwards each read as its own frame and masks each workload read again with a fresh key, so the workload can't choose the bytes on the upstream wire. When a placeholder credential applies to the host, the relay scrubs the `101` head and each server message.

A fault closes both sides and writes one Deny record. The side that broke a rule gets 1002 for a protocol rule or 1008 for a policy refusal. The other side gets 1001 after a workload fault or 1014 after a server fault.

We write the frame codec ourselves on hyper's upgrade, as #230 did. It's about 75 lines of #230's 593-line relay, and the hooks for policy and the scrub are most of the rest. tungstenite and tokio-tungstenite (0.30) answer every Ping and Close themselves, so a workload's Pong or Close never reaches a decision. fastwebsockets (0.8) doesn't check which side masks.

## 3. How does policy decide a connection that stays open?

One decision at the handshake sees none of the data that follows, and the same holds for a streamed response or a large `git push`. We keep the two network actions, `net:connect` and `http:request`, and add none. What changes is when the gateway asks `http:request`.

### 3.1 Exchanges, asks, windows, and records

Four words carry the design:
- An **exchange** is one HTTP/1.1 request and its response, one HTTP/2 stream (one gRPC call), or one WebSocket. On a WebSocket, the request is the workload's frames, and the response is the `101` and the server's frames.
- An **ask** is one `http:request` decision for an exchange. The first ask comes when the request head arrives, where today the gateway asks once after it reads the whole body. Later asks come only if a loaded rule needs more.
- A **window** is the part of an exchange that one admitted ask covers.
- A **record** is the history event that tells what passed in one window, and for how long: `http:request::response`, or `::error` when the action didn't happen.

The first ask writes `http:request::request`, as today. Each later ask, a re-ask, writes `http:request::reask`, so a count of `::request` still counts each exchange once.

Here's a `GET` to `api.github.com` under a policy with a rule on response headers, such as `no_set_cookie` in section 4:
1. The request head arrives. The first ask admits it, and the head leaves at once.
2. The response head arrives and waits. A re-ask judges it, and the same engine call writes the record of window 1, with 0 body bytes each way.
3. The response body passes. When the exchange closes, the box writes the record of window 2, which counts the response body.

On main, the same `GET` makes one decision and one record.

### 3.2 What an ask sees

Each ask carries these input fields. Every field is present from the first ask, as 0 or empty until it's known, so a rule needs no `has` test. With optional fields, a forbid would fail open before its field appears.

| Field | Known | Meaning |
|---|---|---|
| `host`, `port`, `method`, `path`, `intercepted` | At the head | As today |
| `upgrade` | At the head | `"websocket"` on a WebSocket handshake and every later ask of it, `""` otherwise |
| `request_header_names` | At the head | The lower-case names of the fields that the head carries out |
| `status` | From the response head | The final status, 101 on a WebSocket, and 0 before the response head |
| `response_header_names` | From the response head | The lower-case names of the response head's fields, empty before it |

`upgrade` matches the HTTP field name, as `method` and `status` do, so a rule reads like the request: `context.input.upgrade == "websocket"`. A rule that should judge only the response tests `status != 0`. An ask sees no byte count and no duration. Bytes appear only on records.

### 3.3 What a record counts

A record carries four output fields:
- `output.request_body_bytes` counts the request body bytes that passed since the exchange's previous record.
- `output.response_body_bytes` counts the same for the response body.
- `output.duration_ms` counts the milliseconds since the exchange's previous record, or since its first ask for the first record. It's in milliseconds because whole seconds would count a 0.4 s `GET` as 0.
- `output.status` is the response status on the first record written after the response head, and 0 on every other record. So it stays once per exchange, as on main, and a count of `401`s counts exchanges whatever other rules are loaded. The one record of a one-window `GET` holds the final status with no extra ask.

A record also carries its exchange's input fields as known when the box writes it, so `input.status` is on every record after the response head. A rule that sums response bytes or time for one status matches `input.status`, because `output.status` is on only one record. Status can't filter a sum of request bytes, because most of them pass before the response head, so such a sum counts every request byte.

Each admitted ask gets one record. The box writes it at the next ask, in the same engine call, or when the exchange closes. So the records of an exchange add up to its totals and its lifetime, and a budget that sums them sees the bytes and time of open exchanges too.

The counts are payload only, with no head and no HTTP framing. gRPC's 5-byte message prefixes count, because an endpoint writes them. Each WebSocket frame counts 2 bytes for its base header, because its type and end bit reach the other side. The time is connection time, so two exchanges open for one hour each count two hours.

Today an `http:request` input holds `body_bytes`: the body offered on `::request`, and the head plus body that left on `::response`. For a curl `GET` of `/repos/o/r` on `api.github.com`, those are 0 and 124. We remove `body_bytes`, and a rule that reads it refuses to load. A rule that reads a past status still reads `output.status`, for example `formerly within 5m (Box::Action::"http:request"::response{ input.host: "api.example.com", output.status: 401 })`.

A record that the box can't write refuses the ask of its engine call, where main only warns. The box writes `::error` only when no byte of the head left. The `net:connect` before DNS and the decision of a called MCP tool share the record of the decision they refine, as on main.

### 3.4 When a window ends

A window ends only on something that a loaded rule reads:
- **Bytes.** After `window_bytes` (1 MiB) in one direction, if a rule sums that direction's output field.
- **The response head.** If a rule reads `status` or `response_header_names`. The head waits for the re-ask, so the rule acts before any response byte reaches the workload.
- **Time.** After `window_secs` (30 s), if a temporal rule covers `http:request`, as a rule that sums `output.duration_ms` does. A temporal rule reads earlier events, and its lookback, such as `within 10m`, changes as time passes.
- **A move of the history version**, always. This counter moves on a reload, or when a new event could change a guard (section 5).

With none of these, an exchange gets one ask and one record, as on main. Each permit carries the version it saw and the ends that apply. The policy PR (the pull request that changes the policy engine) settles how the engine finds, at load, which fields the rules read.

So that an exchange doesn't wait on policy, the gateway asks ahead. When an exchange has used half of either limit, the gateway asks for the next window, which starts where the current one ends. If the current window ends early, at the response head or a version move, the gateway drops that answer and asks again.

Here's a 2.5 MiB upload under a byte budget, so request bytes end a window every 1 MiB:

| Ask | Comes at | Its engine call writes | Which counts |
|---|---|---|---|
| 1 | The head | No record | |
| 2 | 0.5 MiB | The record of ask 1 | 0.5 MiB |
| 3 | 1.5 MiB | The record of ask 2 | 1 MiB |
| Close | 2.5 MiB | The record of ask 3 | 1 MiB |

The records add up to 2.5 MiB, the body's total.

### 3.5 Refusals

A refused ask is final, and it ends the exchange at once. Before the response head reaches the workload, the workload gets 403 and a Deny record. After it, the response is cut, and a WebSocket closes with 1008. Bytes that pass while a refused ask is decided go in one more record, which counts the time up to the close, so the records still add up. When none pass, as at a waiting response head, there's no such record. Section 7.4 lists every refusal signal.

Box rules are written in Dogwood, the upstream policy language and engine, and Dogwood fixes the event schema of a store. So a box created before the policy PR refuses to start until its operator creates it again.

## 4. What changes in a policy?

Five kinds of rule on `http:request` act at five points:

| Kind | Example below | What it reads | When it acts |
|---|---|---|---|
| Permit | `github_read`, `realtime` | Head fields | At the first ask. A read-only `GET` permit adds `context.input.upgrade == ""` (W1 below). |
| Response check | `no_set_cookie` | `status` or `response_header_names` | At the response head, before any response byte reaches the workload |
| Guard | `none_after_key` | An earlier event, with `formerly` | When the version moves. Each open exchange re-asks before its next write and is refused. |
| Budget | `byte_budget`, `download_budget`, `realtime_time_budget` | The sum of an output field over a lookback | At the next ask after the sum passes the limit, so up to about one window, 1 MiB or 30 s, more passes (limitation 3) |
| Rate limit | `github_rate_limit` | A count of `::request` events | At every ask, so it also cuts open exchanges to its host |

Take a sample policy that loads today and lets an agent call a model API and read from GitHub. `connect`, `model_request`, and `none_after_key`, which forbids every request after a read of `~/.ssh/id_ed25519` in the last 24 hours, need no edit. Three rules change, and four are new.

Dogwood is Cedar plus temporal clauses. `formerly within 10m (P)` holds when an event of the last 10 minutes, the current one included, matches P. `tp(t)` binds `t` to that event's time, and `count` and `sum` add up over the matching events. These rules use fields that the policy PR adds:
```text
@id("github_read")
permit (principal, action == Box::Action::"http:request", resource)
when { context.input.host == "api.github.com" && context.input.method == "GET" &&
       context.input.upgrade == "" };
@id("github_rate_limit")
@description("At most 60 requests to api.github.com started in 10 minutes, refused ones included.")
forbid (principal, action == Box::Action::"http:request", resource)
when { context.input.host == "api.github.com" }
when temporal {
  exists (total: Long). (
    (count for (t: Timepoint). where (
      formerly within 10m (
        Box::Action::"http:request"::request{ input.host: "api.github.com" } && tp(t)
      )
    )) == total && total > 60
  )
};
@id("byte_budget")
@description("At most 10 MiB sent in 10 minutes.")
forbid (principal, action == Box::Action::"http:request", resource)
when temporal {
  exists (total: Long). (
    (sum b for (b: Long), (t: Timepoint). where (
      formerly within 10m (
        Box::Action::"http:request"::response{ output.request_body_bytes: b } && tp(t)
      )
    )) == total && total >= 10485760
  )
};
@id("download_budget")
@description("At most 10 MiB received in 10 minutes.")
forbid (principal, action == Box::Action::"http:request", resource)
when temporal {
  exists (total: Long). (
    (sum b for (b: Long), (t: Timepoint). where (
      formerly within 10m (
        Box::Action::"http:request"::response{ output.response_body_bytes: b } && tp(t)
      )
    )) == total && total >= 10485760
  )
};
@id("realtime")
permit (principal, action == Box::Action::"http:request", resource)
when { context.input.host == "realtime.example.com" && context.input.path == "/v1/realtime" &&
       context.input.upgrade == "websocket" };
@id("realtime_time_budget")
@description("At most 30 minutes of WebSocket time in 1 hour.")
forbid (principal, action == Box::Action::"http:request", resource)
when temporal {
  exists (total: Long). (
    (sum d for (d: Long), (t: Timepoint). where (
      formerly within 1h (
        Box::Action::"http:request"::response{ input.upgrade: "websocket", output.duration_ms: d } && tp(t)
      )
    )) == total && total >= 1800000
  )
};
@id("no_set_cookie")
forbid (principal, action == Box::Action::"http:request", resource)
when { context.input.response_header_names.contains("set-cookie") };
```

The changed rules differ from today in these ways:
- `github_read` adds `context.input.upgrade == ""`, so it admits no WebSocket, though it still admits a `GET` body.
- `github_rate_limit` counts `::request` in place of `::response`, so it counts each exchange once and counts refused attempts too. An agent that retries with no pause then stays refused. Because the count includes the current event, the limit becomes `> 60` in place of `>= 60`, which we tested on a patched copy of main at `93a3761`.
- `byte_budget` sums `output.request_body_bytes` in place of `input.body_bytes`. We tested a sum of this shape on the same copy, when the count still included the head. `download_budget` has the same shape on `output.response_body_bytes`.
- `realtime_time_budget` is new and has the same shape on `output.duration_ms`, over the records of WebSockets only. Its 30 minutes in 1 hour is this sample's choice. Like the byte budgets, it forbids every request once its sum passes the limit.

The rate limit is checked at every ask, with no special case, because an engine that judged counts only at first asks would let an open exchange pass a count guard. The cost is that a burst can cut exchanges that it admitted (section 8).

Two warnings can fire at load. Both stay warnings, because the rule may be what the author means:
- **W1** flags a `GET` permit that doesn't read `upgrade`, because it admits a WebSocket and a `GET` body. It says to add `context.input.upgrade == ""`.
- **W2** flags a count of `http:request::response` records, which counts windows, not exchanges, unless the count matches a nonzero `output.status`. It says to count `http:request::request`. It also flags a `sum` that matches `output.status`, which sees one record of each exchange, and says to match `input.status`. For a sum of `output.request_body_bytes` that matches either status field, it says to drop the status match. Of the shipped policies, only the rate limit in the user docs matches W2, and the policy PR rewrites it.

Two checks refuse a rule at load:
- A rule that covers `http:request` refuses to load if an event it doesn't name can change its result, because nothing could then tell an open exchange to ask again. That covers a few temporal forms, such as `previous`, and no shipped policy has one.
- A rule that names `::reask` refuses to load, so a later engine change that drops the event breaks no rule.

Here's a WebSocket to `wss://realtime.example.com/v1/realtime` under these rules. The workload sends "hello" and two 8 KiB frames, the server sends one 4 KiB frame, and the WebSocket closes after 9 seconds:

| # | When | Event | Input or output |
|---|---|---|---|
| 1 | The head is ready | `http:request::request` | `upgrade` "websocket", `status` 0 |
| 2 | The `101` arrives | `http:request::response` | `output.request_body_bytes` 0, `output.response_body_bytes` 0, `output.status` 101 |
| 3 | In the same call as 2 | `http:request::reask` | `upgrade` "websocket", `status` 101 |
| 4 | The WebSocket closes, 9 s | `http:request::response` | `output.request_body_bytes` 16395, `output.response_body_bytes` 4098, `output.status` 0 |

`no_set_cookie` reads a response field, so the `101` ends window 1 before any frame passes. Event 4 counts the payload plus 2 bytes for each frame: 5 + 8,192 + 8,192 + 3 × 2 = 16,395 one way, and 4,096 + 2 = 4,098 the other. The `output.duration_ms` values of events 2 and 4 add up to the WebSocket's 9 seconds, 9,000 ms. Neither byte count reaches half of `window_bytes`, and the WebSocket closes before 15 s, half of `window_secs`, so it makes no other ask.

The same handshake to `api.github.com` matches no permit, because `github_read` reads `context.input.upgrade == ""`. Its one ask is refused, the head never leaves, and the workload gets 403.

## 5. How does a guard stop an open exchange?

A guard such as `none_after_key` must stop an open exchange as soon as the workload reads the key, but a window can last 30 seconds. So the engine keeps the history version, a counter that #230's branch has and main doesn't. It moves on a reload, and when an appended event matches a trigger.

A trigger is a predicate in a temporal clause of a rule that covers `http:request`, outside a `sum` or a `count`. In `formerly (P)` and `!P since Q`, the P and the Q are triggers. An event matches a trigger when it has the trigger's action, kind (such as `::response`), and literal fields. A variable, `_`, or a `context.input` field matches any value.

The relay compares the version before each write in both directions. If it moved, the exchange asks again before the write:

| Time | The workload | Version | The gateway |
|---|---|---|---|
| 0 s | Opens `wss://realtime.example.com` | 7 | First ask, then the ask at the `101` |
| 1 s | Sends 8 KiB of audio | 7 | Same window, no ask |
| 2 s | Reads `~/.ssh/id_ed25519` | 8 | The `fs:read` record matches the trigger |
| 3 s | Sends "key=..." | 8 ≠ 7 | Re-ask refused, close 1008 |

The frame at 3 s never leaves, and other `fs:read` events move nothing. #230's version moves on every event except those of `net:connect` and `http:request`, so a guard on requests to a host would miss open exchanges there. Triggers by kind alone would make every open exchange ask again at each `fs:read`.

Three choices keep the version cheap:
1. A trigger that is the whole operand of a `formerly`, with fields that are all literals or `_`, moves it only at its first match in its lookback, because a later match can't change that `formerly` result. Every other trigger moves it at each match: one with a variable or a `context.input` field, and one that shares its `formerly` with another conjunct, a nested temporal clause, or a `tp` binding. There a later match can turn the result from false to true, as in "read the key, then run `tar`".
2. A `sum` or a `count` is never a trigger, so it acts at the next window, within `window_secs`.
3. One engine call writes the previous window's record and decides the re-ask, under one hold of the engine lock. It returns the version after its own appends, so an exchange never asks again because of its own events. #230 moves the version outside the lock, and the policy PR doesn't port that.

## 6. What does it cost?

A window costs one ask and one record. The numbers come from two sources:
- #230 measured its asks with `box-bench` through a real box on macOS, against the same relay with no asks. Each ask there decided a 64 KiB lease, #230's name for its window, and wrote the record of the lease before it.
- A probe on a patched copy of main at `93a3761`, in a release build on macOS, measured the engine with the sample policy of section 4 as it loads today, or with its permits only.

| Measurement | Result, p50 unless stated |
|---|---|
| #230, a 64 B round trip on one WebSocket | 0.140 ms, against 0.139 ms with no asks |
| #230, a 64 KiB message round trip | 6.877 ms, against 0.698 ms with no asks |
| #230, new WebSockets that each send one message | 54.1 per second, against 72.9 with no asks |
| Probe, sample policy, up to about 3,400 events | Ask 4.0 to 4.2 ms, record 2.7 to 3.3 ms |
| Probe, sample policy, about 16,500 events | Ask 21.4 ms (p99 69.5 ms), record 3.4 ms |
| Probe, permits only, about 16,500 events | Ask 4.0 ms |

So a window costs about 6.7 to 7.5 ms at a small history, and about 25 ms at 16,500 events. One mutex guards each engine call, so the whole box makes about 133 to 149 windows a second at a small history, which is 133 to 149 MiB/s in 1 MiB windows. #230's 64 KiB lease would give about 8 to 9 MiB/s.

For one exchange, ask-ahead hides each ask up to about 67 to 75 MiB/s, where half a window takes as long to pass as an ask and a record. Time asks from 256 exchanges, 8.5 a second, use about 6 percent of the engine. A response check adds one ask at each response head, which ask-ahead can't hide.

The growth of the history is the cost I'm least sure of. A `count` or a `sum` rescans its lookback, which adds about 1.2 µs to an ask for each event in it. Each window adds two events, so a 1 GiB body under a byte budget adds about 2,050. At full rate under the sample policy, a box adds 16,500 events to a 10-minute lookback in about a minute. It then slows to near 24 windows a second across all exchanges, about 24 MiB/s, with each ask taking about 39 ms. We haven't measured that state, and the policy PR's bench streams for more than 10 minutes and reports ask time and engine memory.

## 7. How does the same model apply to HTTP?

### 7.1 Bodies and responses

HTTP uses the same action and windows as a WebSocket. Every body and response streams with backpressure, except for three consumers that need a whole body:
- A signing host, with an `aws://` profile or a `credsd://` source, because the signer hashes it.
- A remote MCP host, because `mcp:call` parses the request.
- A `tools/list` response, which the gateway reads for the tool catalog.

The gateway reads these up to `max_whole_body_bytes`, 16 MiB, main's `max_body_bytes`, so no body that main sends gets 413. A body over it gets 413 before the first ask, and a `tools/list` response over it gets 502. A whole body's record counts it as any other. We rejected a key per host for whole or streamed bodies, because one rule would then mean different things on two hosts.

When a re-ask refuses a streamed body, the gateway resets or closes the upstream, so the server never sees a complete body. The gateway answers `Expect: 100-continue` itself after the first ask, and every response keeps the upstream's `Content-Length`, so a client can tell that a cut response is short.

The scrub runs when a placeholder credential applies to the host. It writes a mask of the same length as the secret, where main writes `[REDACTED]`, because only that keeps `Content-Length` and every length prefix valid. The scrub can't read compressed bytes, so a compressed response from a credentialed host, one whose credential the gateway attaches, gets 403 before any byte reaches the workload.

The scrub holds back the last L - 1 bytes of each read, where L is the length of the longest secret bound to the host, and releases them with the next read or at the end, never on a timer. When no bound secret holds a control byte, a space, a double quote, or a backslash, it releases everything up to the last such byte, so a server-sent event or a JSON stream waits for nothing.

### 7.2 HTTP/2

Both legs speak the HTTP version that the upstream picks, so nothing is translated. The gateway reads the workload's ClientHello with tokio-rustls's `LazyConfigAcceptor`, offers the upstream the same ALPN list (Application-Layer Protocol Negotiation) limited to `h2` and `http/1.1`, and offers the workload only the upstream's choice. HTTP/2 without TLS (h2c) stays refused.

Each stream is one exchange, with `method` and `path` from `:method` and `:path`, so a rule on `path` scopes one gRPC method. `:authority` gets today's `Host` checks, and a mismatch with the `CONNECT` host gets 421. Each workload connection gets its own upstream connection, because a pool would let a stream use a connection that another decision opened. A connection carries up to 100 streams at a time, the smallest limit that RFC 9113 recommends, so a `net:connect` count counts connections, not exchanges.

Response trailers pass for gRPC, and the scrub and the header caps cover them, but policy doesn't see them. Request trailers get 400. Every shape of gRPC call works except a client-streaming or two-way call to a signing host.

Either leg can be hostile, so the gateway guards both:
- It checks the decoded header list against `max_header_bytes` itself, which stops an HPACK bomb (CVE-2016-6581).
- It bounds the CONTINUATION frames of one header block, and sends GOAWAY with `ENHANCE_YOUR_CALM` after too many reset streams (CVE-2023-44487).
- Stream flow-control limits are 65,535 bytes on the workload leg, the RFC 9113 default, and 2 MiB upstream, hyper's client default.
- A credential header goes upstream as a never-indexed literal, and server push is off.

### 7.3 Headers

Policy reads header names, never values, because a value can hold a placeholder, a cookie, or a key, and values in policy would put credentials in the history. `request_header_names` holds the names that leave, after the hop-by-hop strip and the operator's edits, and before the credential. `response_header_names` holds the names after the scrub and the operator's edits. Request trailers get 400, so a rule on presence covers every field that leaves:
```text
@id("no_cookies_out")
forbid (principal, action == Box::Action::"http:request", resource)
when { context.input.request_header_names.contains("cookie") };
```

To strip a field and keep the request, an operator edits the head in config, because policy only decides and never changes bytes. `headers.request.remove`, `headers.request.set` for public literals, and `headers.response.remove` run before the credential and SigV4, so the signature covers the edited head. An edit that names a protected field, such as `host`, a framing field, or the host's secret header, refuses to load.

`max_header_bytes` (65,536) and `max_header_fields` (64) keep main's numbers and become keys. They cover heads, trailers, and a decoded HTTP/2 list. Over a cap, the workload gets 431 for its own head or 502 for the upstream's. Today hyper answers 431 with no Deny record, and an upstream head over the cap drops the connection.

### 7.4 Refusals, clocks, and slots

| Cause | HTTP | gRPC | WebSocket |
|---|---|---|---|
| A rule refuses | 403 | 7 | 1008 |
| A response that the scrub can't read | 403 | 7 | n/a |
| A protocol rule, such as bad framing or request trailers | 400 | 13 | 1002 |
| A `CONNECT` inside a stream | 405 | n/a | n/a |
| A whole body over `max_whole_body_bytes` | 413 | 8 | n/a |
| The authority differs from the `CONNECT` host | 421 (main answers 403) | 14 | n/a |
| A workload head over a header cap | 431 | 8 | n/a |
| Idle, with the request incomplete or complete | 408 or 504 | 4 | 1001 |
| An upstream fault, an upstream TLS failure, an upstream head over a cap, or a `tools/list` response over `max_whole_body_bytes` | 502 | 14 | 1014 |
| No free slot | 503, or `REFUSED_STREAM` for a stream | `REFUSED_STREAM` | n/a |

Each refusal writes one Deny record and gives one signal from this table. Before the response head, a gRPC code goes in gRPC's Trailers-Only form, because a gRPC client maps most HTTP statuses to `UNKNOWN`.

Every response that the gateway writes carries a marker, the `x-strands-box-egress` field, and its body names the rule or the config key. Box's kernel wrapper turns a marked response into a permission-denied error, so it then reports a 502 or 503 from the gateway too. Today only a refusal carries the marker.

On HTTP/1.1, an immediate close after a refusal could reach the client as a reset before it reads the status. So the gateway reads and discards for at most 30 s, with at most 5 s between reads (nginx's defaults), before it closes.

`idle_secs` (300 s) is the only clock. On main, `upstream_read_deadline` is a 300 s total for each exchange, which would cut every long stream. `idle_secs` keeps the 300 s as a limit on idle time on both legs, as Envoy's `stream_idle_timeout` default does.

A slot is one place under the gateway's connection limit: one for each workload connection, and one more for each open HTTP/2 stream after the first. `max_slots` caps them at 256, main's `max_connections`. With no free slot, the gateway answers 503 at once, where main makes the connection wait. Each slot holds one read each way (64 KiB), so 256 slots take about 32 MiB, plus any whole body.

At shutdown, open relays end with no Close frame and no record of their last bytes. The plumbing PR settles whether to add a drain that sends 1001 both ways and records the last bytes.

## 8. Five exchanges

These exchanges run under the rules of section 4, with no `byte_budget` or `download_budget` except where stated. `no_set_cookie` makes each exchange ask again at its response head. Each one ends before 15 s, where a time ask would come, unless the row says otherwise.

| Exchange | Asks | Records | What happens |
|---|---|---|---|
| An HTTP/1.1 `GET` to `api.github.com` | 2 | 2 | The example of section 3.1. |
| A 40 MiB chunked `git push` to `github.com` at 10 MiB/s (a guess), under `byte_budget` | 12 | 12 | Asks come at the head, at 0.5 MiB, and at each MiB after it, so ask 12 comes at 10.5 MiB. Its engine call writes the record of ask 11, which brings the records to 10.5 MiB, so ask 12 is refused, and the workload gets 403. Asks 1 to 11 have a record each, and one last record counts the bytes that left while ask 12 was decided. Without `byte_budget`, the push makes 2 asks and 2 records and completes. |
| A 1 GiB download with `Content-Length` at 10 MiB/s (a guess), under `download_budget` | 13 | 13 | Asks 1 and 2, at the head and the response head, pass. Asks then come at 0.5 MiB of the body and at each MiB after it, so ask 13 comes at 10.5 MiB, sees records of 10.5 MiB, and is refused, and the response is cut. Asks 1 to 12 have a record each, plus one last record. No rule can refuse the declared 1 GiB at the head. |
| 70 `GET`s to `api.github.com` in one minute, one after another, while a download from it is open | 2 for each admitted `GET` and 1 for each refused one, plus the download's refused ask | 2 for each admitted `GET` | The download's `::request` counts once, so `GET`s 60 to 70 see counts over 60 and get 403. The download's next ask, at the latest its time ask, also sees a count over 60, so the download is cut. |
| 61 `GET`s to `api.github.com` sent at once, with no other request to it in 10 minutes | 2 for each of the first 60, and 1 for the 61st | 1 for each of the first 60 | The first asks see counts 1 to 61, so the 61st gets 403. Each response-head ask then sees 61, so all 60 get 403 before their response heads reach the workload, and 0 of 60 complete. With 60 `GET`s, all 60 complete. |

## 9. Configuration

Budget rules limit bytes, while config sets limits and header edits and never admits a byte. Box-wide keys go in a new top-level table, `[gateway]`, and header keys go on the destination entry, `[egress.<name>]`. Every key is optional, and a value out of range refuses to load, with the ranges set by the limits PR. `window_bytes` has a floor, so one window always holds the largest WebSocket control frame. An entry that sets a header key no longer needs a `secret`.

| Key | In | Default and its source | What it sets |
|---|---|---|---|
| `idle_secs` | `[gateway]` | 300, main's total for one exchange | How long nothing may move on either leg |
| `window_bytes` | `[gateway]` | 1 MiB, a guess | The most payload that one window lets pass each way |
| `window_secs` | `[gateway]` | 30, #230's lease | The longest time that one window lasts |
| `max_whole_body_bytes` | `[gateway]` | 16 MiB, main's `max_body_bytes` | The largest body that the gateway reads whole |
| `max_header_bytes` | `[gateway]` | 65,536, main's `MAX_HEAD_BYTES` | The largest head or trailer section |
| `max_header_fields` | `[gateway]` | 64, as main | The most fields in one head or trailer section |
| `max_slots` | `[gateway]` | 256, main's `max_connections` | The total of slots |
| `headers.request.remove`, `headers.request.set`, `headers.response.remove` | `[egress.<name>]` | None | Header fields to remove, or to set to public literals |

The 64 KiB relay read, the 100 HTTP/2 streams, the flow-control limits, the lingering close, and the no-slot answer are fixed and published but aren't keys. We leave out a header cap per host, which has no measurement behind it, and HTTP/1.1 keep-alive, because every HTTP/1.1 client works with one request per connection.

## 10. How do we deliver it?

The work lands in seven PRs, and each fails closed, which means it refuses what it doesn't yet decide. The gateway asks policy through one interface, `EffectInterceptor`, about an `EffectAttempt`, which the box's policy adapter maps to an action.
1. This doc.
2. The limits PR adds `[gateway]` with main's numbers and `idle_secs`, a status, the marker, and a Deny record for each cap, 421, the immediate 503, and the lingering close.
3. The plumbing PR adds the relay with its frame rules and scrub, and the windows with ask-ahead. The gateway sends each handshake as an `HttpRequest` with `upgrade` "websocket", with no new effect. Until the policy PR the adapter refuses that and every re-ask, so a real box refuses every handshake with 403.
4. The policy PR changes the engine with no Dogwood change and no pin bump. It adds `::reask`, the version with its triggers, the one call that writes the record and decides the re-ask, and the load checks. The schema gains the four input fields, adds `output.request_body_bytes`, `output.response_body_bytes`, and `output.duration_ms`, keeps `output.status`, and loses `body_bytes`. The PR adds end-to-end tests and the bench through a real box. In the user docs, "A refused request does not count" becomes "Each attempt counts, refused ones too, so an agent that retries with no pause stays refused."
5. The streaming PR streams bodies and responses window by window, and adds the stream scrub, the encoding check, and the `Expect` answer. It removes main's total clock and `ResponseRelease`. It follows the policy PR, so no byte streams before a rule can judge it.
6. The headers PR adds the two header-name fields and the header edits.
7. The HTTP/2 PR adds HTTP/2 by ALPN, with its limits, trailers, and the gRPC signals.

The policy, streaming, and HTTP/2 PRs change what an older policy admits. These are the changes to existing behavior:
- An `http:request` permit admits the whole exchange, so a body or a response over 16 MiB passes under it.
- No rule can cap one exchange's size or lifetime, or refuse a declared `Content-Length` at the head.
- A `GET` permit admits a WebSocket and a `GET` body unless it reads `context.input.upgrade == ""`.
- The shipped rate limit counts `::request`, ends with `> 60`, and counts refused attempts.
- A rate limit is checked at every ask, so it also cuts open exchanges to its host. A burst of 61 parallel `GET`s under a limit of 60 completes none of them.
- A rule that reads `body_bytes` refuses to load, and so do some temporal forms. `output.status` stays once per exchange, on the first record after the response head. A byte budget doesn't count heads.
- A box made before the policy PR refuses to start until its operator creates it again, and a record that can't be written refuses its ask.
- With HTTP/2, a `net:connect` count counts connections.
- The marker on every gateway response, the immediate 503, `idle_secs`, 421, and the same-length mask replace main's behavior (section 7).

If we accept this design, we approve these changes to frozen surfaces, the interfaces that change only with an approved design:
- The egress gateway's `pub use` set (all six code PRs). `EffectAttempt` becomes `Resolve`, `Connect`, and `HttpRequest`, and `ResponseRelease` goes with its README row. `HttpRequest` gains the four input fields and loses `body_bytes`. `EffectInterceptor` gains a history version query and a re-ask call, with no default, and `MitmConfig` gains fields. In the docs, "No attempt carries a header" becomes "no header value", and "`body_bytes` is a length" goes.
- What `EffectAttempt` fields mean (the policy and streaming PRs). There's one attempt for each ask, and the permit carries the version and the ends that apply. Every exit records the body bytes that passed each way and the time since the last record, never the 0 bytes that a dropped `EffectPermit` records today.
- The policy crate's `pub use` set (the policy PR). `PolicyEngine` gains the version query and the re-ask call, and the `open`, `decide`, `record`, `effective` lifecycle stays. `Request::Http` loses `body_bytes` and gains the same four fields. `Outcome::Http` keeps `status` and adds three counts, the two byte counts and the duration in milliseconds, with no new variant.
- What `when temporal` observes (the policy PR): one record for each admitted ask, a `::reask` event for each re-ask, and a version that moves on triggers.
- The policy action vocabulary (the policy and headers PRs): the four input fields, `output.request_body_bytes`, `output.response_body_bytes`, `output.duration_ms`, `output.status`, and `::reask`, with no `body_bytes`. The load checks gain the refusals, W1, and W2.
- `box.toml` (the limits, plumbing, and headers PRs): the `[gateway]` table with seven keys, and header keys on `[egress.<name>]`.

## 11. Key decisions

### D1. `http:request` decides the whole exchange, window by window

One action covers the head, the request bytes, and the response, on every transport and host. A budget is then one rule that sums one output field, and a guard stops every byte before the next write. The cost is that an older permit now admits the whole exchange. We rejected three options:
- #230's option A gave `http:request` an optional `upgrade` field and set `body_bytes` to the bytes allowed next. A `GET` permit admitted an uploading WebSocket with no warning, and a rate limit counted each ask.
- Option B gives each transport its own actions, such as `websocket:open` and `websocket:send`. Each later transport would need its own actions, and a rule that reads `context.input` on actions of two families refuses to load.
- Option C uses `http:request` for each head and `http:send` for the bytes after it. A guard on `http:request` alone would miss open exchanges, a budget over both would need a Dogwood change, and responses would stay outside every rule.

### D2. A `GET` permit admits a WebSocket and a `GET` body, with warning W1

A read-only permit adds `context.input.upgrade == ""`. A `GET` body's content can't change the meaning of the request (RFC 9110), and budgets and guards count its bytes. Refusing every WebSocket that no rule names would be a hidden rule that no author wrote.

### D3. Windows of 1 MiB and 30 s that end only on what rules read, with ask-ahead

Windows of 64 KiB, as in #230's lease, would ask 16 times as often. Windows that end on every field would add asks that no rule needs, and asking only at a window's end would stall each exchange once a window. `window_bytes` (1 MiB) is my guess, and `window_secs` (30 s) is #230's lease time. Both are box-wide, so a budget's overshoot is the same on every host. They change the precision and the cost of a rule, never what it decides, and they're where I'd most like input.

### D4. A response is decided as part of its exchange

Config sets no response cap, because main's 16 MiB limit comes from the size of its buffer, and no rule chose it. A download budget and a response check cover responses. A `max_response_bytes` key can't tell one host from another, and a response action would make each rule on a host name two actions.

### D5. A re-ask writes an `http:request::reask` event now

The other choice is a Dogwood call that decides without writing an event. That would halve the rescans, but it needs an upstream change of unknown size and a pin bump. The cost of `::reask` is that an older box must be created again. If we later move to that call, no rule breaks, because no rule may name `::reask`. Writing `::request` for a re-ask would make every count of `::request` count windows. Resetting an older history would forget every guard, so a box that read the key before the upgrade could send it after.

### D6. HTTP/2 follows ALPN, and both legs speak one version

With no translation, no HTTP/2 field is ever written into HTTP/1.1 text. The cost is that a `net:connect` count counts connections. A version key per host would have to match what the workload offers before the gateway sees a path. A box switch that's off by default would keep gRPC broken until an operator found it, and we found no workload that needs translation.

## 12. Limitations

1. The scrub reads one message at a time, so it misses a secret split across two messages or encoded again, for example in base64. A mask in a binary stream breaks the stream and shows the secret's length. Held bytes can stall a two-way binary stream until `idle_secs`, and a signing host has no scrub.
2. There's no WebSocket over HTTP/2 (RFC 8441), no WebSocket compression, and no HTTP/3, which runs on UDP. With HTTP/2, a `net:connect` rule sees one event for many exchanges, so write guards and rate limits on `http:request`.
3. No rule can cap the size or the lifetime of one exchange, or refuse a declared `Content-Length` at the head, because an ask sees no byte count and no time. `idle_secs` is still the only clock that the gateway applies itself. A budget acts at the next ask after its sum passes the limit, so usually about one window more passes: 1 MiB for a byte budget, or 30 s for a time budget. When a time ask comes first, an exchange can pass up to 1.5 × `window_bytes` between asks, plus the bytes that pass while the refused ask is decided. So each open exchange can pass a budget by at most 2 × `window_bytes`, which is 512 MiB across 256 exchanges.
4. A rate limit is checked at every ask, so once a count passes the limit, it cuts the open exchanges to that host too. A burst of parallel requests over the limit can then complete none of them (section 8).
5. A byte budget doesn't count heads, so data in a URL or header fields is outside it. `max_header_bytes` (65,536) caps each head, and a rate limit caps how many heads leave.
6. An ask costs more as the lookback grows, and we haven't measured the state near 24 windows a second. There, a guard with a 24h lookback keeps a timepoint for each event, about 4.1 million at two events a window. In #230's bench the first ask took new WebSockets from 72.9 to 54.1 per second, about 4.8 ms more for each.
7. A `formerly` guard acts before the next write once its record is written. The Strands Shell and the Python broker write the `fs:read` record before the read returns. The experimental native filesystem policy on macOS writes it after the kernel admits the open, so a write right after the read can leave first, as on main.
8. A `tools/list` response stays whole, so a server that answers it with server-sent events delivers only at the end. An upload to a signing host stays whole up to `max_whole_body_bytes`, and the gateway doesn't sign `aws-chunked` uploads, so a larger upload gets 413. We haven't tested whether main's signer handles an SDK upload sent as `aws-chunked` with a trailing checksum.
9. Memory has a bound, but a large one. Whole bodies take at most 256 × 16 MiB = 4 GiB. HTTP/2 flow control adds at most 512 MiB upstream and 16 MiB on the workload leg, and relay reads add 32 MiB.
