package mcp;

import java.io.IOException;

import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;
import io.spring.streamsource.task.ActiveSource;

/**
 * The client's view of the network: send a request, get back the {@link
 * Source} of whatever a remote party sends in response.
 */
@FunctionalInterface
public interface NetworkClient {

	Source<String> call(String request);


	static NetworkClient createWithResponse(MockResponseHandler handler) {
		return request -> ActiveSource.<String>fromProducer(sink -> {
			try {
				handler.handle(request, sink);
			}
			catch (Throwable ex) {
				sink.completeExceptionally(ex);
			}
		}).start();
	}


	interface MockResponseHandler {

		void handle(String request, Sink<String> notifications) throws IOException, InterruptedException;

	}

}
