package mcp;

import io.spring.streamsource.Sink;

/**
 * Answers one request, pushing zero or more notifications to {@code notifications}
 * before returning the response.
 * For Streamable HTTP this means the server is answering with
 * Content-Type: "text/event-stream".
 */
@FunctionalInterface
public interface EventStreamHandler {

	String stream(String request, Sink<String> notifications) throws Exception;

}
