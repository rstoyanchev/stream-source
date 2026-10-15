# Exposing MCP Streamable HTTP with Source<T>

This sample explores stream-source support for MCP's Streamable HTTP transport
(2026-07-28 protocol), where a server answers one request either with a single
JSON value or with a `text/event-stream` of zero or more notifications followed
by a final response. 

`NetworkClient` returns `Source<String>` with mock data that emulates a network stream.
`McpClient` is a test client that consumes the network stream and provides
options to receive the response and/or notifications.

`McpServer` is a test server that finds the `SingleResponseHandler` or `EventStreamHandler`
registered for a given request, and calls it to produce a `Source<String>` response.

## Implement a server

[McpServer](McpServer.java) registers two kinds of handler per request name.
[SingleResponseHandler](SingleResponseHandler.java) answers with one value and
has nothing to stream. [EventStreamHandler](EventStreamHandler.java) pushes
zero or more notifications into a `Sink<String>` before returning its response:

```java
McpServer server = new McpServer()
        .respondTo("hello", request -> request.toUpperCase())
        .streamOn("chunk", (request, notifications) -> {
            notifications.send(request + "-1");
            notifications.send(request + "-2");
            return request + "-3";
        });
```

`server.call(request)` runs the matching handler on an `ActiveSource`'s virtual
producer thread and returns the resulting `Source<String>`: notifications, then
the response as the final item, then completion. A handler failure calls
`completeExceptionally()` instead, reaching the caller through the same
`receiveNext()`/`next()` path as a normal end-of-stream -- not a separate error
flag. Because it is an `ActiveSource`, closing the `Source` early interrupts the
handler thread and waits for it to actually stop before returning: cancellation
is ordinary `AutoCloseable`, demonstrated in
`closingCancelsTheHandlerThreadInsteadOfLeakingIt`.

The runnable version is [McpServerTests](../../../test/java/mcp/McpServerTests.java). Run the
sample suite from the repository root:

```shell
./gradlew :experiments:test --tests 'mcp.*'
```

## Consume a call from the client

[McpClient](McpClient.java) offers three views over the one `Source` a
`NetworkClient` hands back for a request.

`call(request)` is the request/response view: it drains the call to its natural
end and keeps the last item as the response, silently discarding whatever
notifications came before it.

```java
String response = client.call("hello");
```

`stream(request)` returns the raw `Source` itself, undemultiplexed:

```java
try (Source<String> responses = client.stream("chunk")) {
    while (responses.receiveNext()) {
        System.out.println(responses.next());
    }
}
```

Nothing here tells a notification from the response other than that the
response is whatever came last before the stream ended -- the caller does its
own demultiplexing, one item at a time.

`call(request, notifications)` sits between the two: it pushes every item ahead
of the response into a caller-supplied `Sink<String>` as it arrives, then
returns the response once the call ends naturally. Handing it the write end of
a `BlockingQueueExchange` lets the caller read notifications from a separate
thread while this call is still waiting for the response:

```java
BlockingQueueExchange<String> notifications = new BlockingQueueExchange<>();
String response = client.call("chunk", notifications);
```

### Why this is a compromise

This is still one `Source` pulled on one thread, and there is no other way to
tell a notification from the response than to look at what follows it. Nothing
ties this call's lifetime to whatever is reading the other end of
`notifications`: if that reader is gone, `send()` eventually blocks forever
once its bounded queue fills, and if the call itself fails, the reader learns
only through `completeExceptionally()` -- there is no cancellation flowing the
other way. `callWithASideChannelBlocksForeverWhenTheNotificationReaderIsGone`
demonstrates the hang directly. A `StructuredTaskScope` would let the pull loop
and the caller's notification handling run as sibling subtasks instead, joined
under a shared cancellation policy, so closing the scope guarantees both are
done -- no thread left unjoined, no `send()` stuck because its reader died. See
the javadoc on `McpClient.call(String, Sink)` for the fuller argument.

## Fan-out to independent subscribers

A streaming handler can also model pub/sub: each call to `server.call(topic)`
registers its `Sink<String>` with a `MultiSink`, and sending an update into
the `MultiSink` delivers it to every registered subscriber, dropping one that
can no longer accept it instead of stopping delivery to the rest. Cancelling one client's
`Source` -- an ordinary `close()` -- deregisters only that subscriber; the
others keep receiving. See `broadcast_publishesToEverySubscriberAndCancellingOneLeavesTheRestUnaffected`
and `broadcast_aSubscriberThatCanNoLongerAcceptUpdatesDoesNotStopDeliveryToTheRest`
in [McpServerTests](../../../test/java/mcp/McpServerTests.java).

## Client and server, driven together

[McpClientTests](../../../test/java/mcp/McpClientTests.java) exercises `McpClient` against a
scripted `NetworkClient`. `McpServerTests` exercises `McpServer` directly,
reading straight off the `Source` it returns, with no `McpClient` in between --
each test picks whichever half is under test rather than always wiring both
together end to end.
