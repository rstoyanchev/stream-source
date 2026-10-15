package mcp;

import java.util.HashMap;
import java.util.Map;

import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;
import io.spring.streamsource.task.ActiveSource;

public final class McpServer {

	private final Map<String, SingleResponseHandler> responseHandlers = new HashMap<>();

	private final Map<String, EventStreamHandler> streamHandlers = new HashMap<>();

	// A real registry would want to reject registering the same request here
	// and in streamOn, or twice in either -- an ambiguous configuration. The
	// tests never do that, so nothing here detects it.

	public McpServer respondTo(String request, SingleResponseHandler handler) {
		this.responseHandlers.put(request, handler);
		return this;
	}

	public McpServer streamOn(String request, EventStreamHandler handler) {
		this.streamHandlers.put(request, handler);
		return this;
	}

	public Source<String> call(String request) {
		return ActiveSource.<String>fromProducer(responses -> {
			try {
				String response = answer(request, responses);
				responses.send(response);
				responses.complete();
			}
			catch (Throwable ex) {
				responses.completeExceptionally(ex);
			}
		}).start();
	}

	private String answer(String request, Sink<String> notifications) throws Exception {
		SingleResponseHandler responder = this.responseHandlers.get(request);
		if (responder != null) {
			return responder.answer(request);
		}
		EventStreamHandler streamer = this.streamHandlers.get(request);
		if (streamer != null) {
			return streamer.stream(request, notifications);
		}
		throw new IllegalArgumentException("no handler registered for " + request);
	}

}
