# Spring extensions

Experimental support for use of [stream-source](../README.md) in Spring projects.

**Expected coordinates:** `org.springframework.experimental.streamsource:stream-source-spring-extensions`\
**Dependencies:** `stream-source`, `spring-web`

Currently, the module provides an SSE implemenation of `Source` for `RestClient`.
It uses HTTP messages converters to decode events.

See [SseIntegrationTests](src/test/java/org/springframework/experimental/streamsource/web/client/support/SseIntegrationTests.java) for example usage.

