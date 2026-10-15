package mcp;

/**
 * Answers one request with a single value; there's nothing to stream.
 * For Streamable HTTP this means the server is answering with
 * Content-Type: "application/json".
 */
@FunctionalInterface
public interface SingleResponseHandler {

	String answer(String request) throws Exception;

}
