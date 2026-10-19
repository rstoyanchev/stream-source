## Overview

[Virtual Threads](https://openjdk.org/jeps/444) made blocking cheap.
[Structured Concurrency](https://openjdk.org/jeps/543) will simplify concurrent code.
Modern applications also need a way to stream data (SSE, LLM tokens, MCP responses, etc.).
There is currently no simple, standard way to do that in Java in imperative style.

Historically in Java, we have streamed data through callback interfaces.
Callbacks are good for asynchronous handling.
For imperative code we prefer a simple loop that blocks.
Like `Iterator` but for data from a remote host.
Like `InputStream` but for Objects.
More than API style, we need support for independent producer-consumer, and other essential patterns.

*Stream Source* is a small library that aims to fill this gap.
It is built by the Spring team but does not depend on Spring.
A core facility for the Java ecosystem with wide applicability.

The Structured Concurrency JEP mentions "channels" are a non-goal but may be proposed in the future.
We aim to fill that gap in the interim with a solution for the use cases we see.
When there is a concrete proposal in the JDK, we will adapt accordingly.


## What's in the repository?

### stream-source
**Dependencies:** `jspecify`, `slf4j-api`

Provides key contracts and building blocks for data streaming.
See [DESIGN.md](DESIGN.md) and [FAQ.md](FAQ.md).

```xml
<dependency>
    <groupId>io.spring.streamsource</groupId>
    <artifactId>stream-source</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### experiments

Concrete experiments to model expected use cases:

- [MCP Streamable HTTP](experiments/src/main/java/mcp/README.md)


## Other repositories?

- [Spring AI Agent](https://github.com/sdeleuze/spring-ai-agent) -- 
  Agentic abstraction for Spring AI that uses *stream-source*.
- [Stream Source Spring Extensions](https://github.com/spring-projects-experimental/stream-source-spring-extensions) --
  Spring project extensions that use *stream-source*.


## Requirements and Use

`stream-source` has a Java 17+ baseline.
Virtual threads are used by default on Java 21+.

The published artifacts use the `io.spring.streamsource` group ID, and can be installed
locally with `./gradlew publishToMavenLocal`.
