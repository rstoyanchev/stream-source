package mcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;
import io.spring.streamsource.task.MultiSink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link McpServer} on its own: requests come in through {@link McpServer#call}, pretending
 * to be whatever a real network would hand it, and everything it produces is read back
 * off the {@link Source} it returns directly -- no {@link McpClient} involved. Which
 * shape a given call takes depends on where the server finds a handler for it: a plain
 * request/response handler registered with {@link McpServer#respondTo}, or a streaming
 * one registered with {@link McpServer#streamOn}.
 * <p>
 * Cancelling is just closing the {@code Source}: {@link
 * io.spring.streamsource.task.ActiveSource#start()} makes {@code close()} interrupt
 * the handler thread and wait for it to actually stop before returning, so
 * cancellation is ordinary {@code AutoCloseable}. Errors are ordinary
 * Java exceptions: a handler that throws ends up calling {@link
 * Sink#completeExceptionally}, and the same exception comes back out of the caller's
 * {@code receiveNext()}/{@code next()} &mdash; the same path a normal end-of-stream
 * takes, not a separate error-flag protocol.
 */
class McpServerTests {

	private static List<String> drain(Source<String> source) throws Exception {
		try (source) {
			List<String> items = new ArrayList<>();
			while (source.receiveNext()) {
				items.add(source.next());
			}
			return items;
		}
	}

	// -- request/response -----------------------------------------------------

	@Test
	@Timeout(2)
	void respondToAnswersARegisteredRequest() throws Exception {
		SingleResponseHandler upperCase = String::toUpperCase;
		McpServer server = new McpServer().respondTo("hello", upperCase).respondTo("world", upperCase);

		assertThat(drain(server.call("hello"))).containsExactly("HELLO");
		assertThat(drain(server.call("world"))).containsExactly("WORLD");
	}

	@Test
	@Timeout(2)
	void handlerFailurePropagatesAsARealExceptionInsteadOfAResultFlag() throws Exception {
		SingleResponseHandler handler = request -> {
			if (request.equals("boom")) {
				throw new IllegalStateException("handler failed");
			}
			return request.toUpperCase();
		};
		McpServer server = new McpServer().respondTo("boom", handler).respondTo("ok", handler);

		assertThatThrownBy(() -> drain(server.call("boom")))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("handler failed");

		// each call is its own Source, so a failed one doesn't poison the next
		assertThat(drain(server.call("ok"))).containsExactly("OK");
	}

	@Test
	@Timeout(2)
	void callThrowsWhenNoHandlerIsRegisteredForTheRequest() {
		McpServer server = new McpServer();

		assertThatThrownBy(() -> drain(server.call("nobody registered this")))
				.isInstanceOf(IllegalArgumentException.class);
	}

	// -- request-stream-response-termination ----------------------------------

	@Test
	@Timeout(2)
	void streamOnSendsNotificationsThenTheResponse() throws Exception {
		McpServer server = new McpServer().streamOn("chunk", (request, notifications) -> {
			notifications.send(request + "-1");
			notifications.send(request + "-2");
			return request + "-3";
		});

		assertThat(drain(server.call("chunk"))).containsExactly("chunk-1", "chunk-2", "chunk-3");
	}

	@Test
	@Timeout(2)
	void streamingHandlerFailureAfterSomeItemsPropagatesInsteadOfHanging() throws Exception {
		McpServer server = new McpServer().streamOn("chunk", (request, notifications) -> {
			notifications.send("1");
			notifications.send("2");
			throw new IOException("connection reset by peer");
		});

		List<String> received = new ArrayList<>();
		try (Source<String> responses = server.call("chunk")) {
			assertThatThrownBy(() -> {
				while (responses.receiveNext()) {
					received.add(responses.next());
				}
			}).isInstanceOf(IOException.class).hasMessage("connection reset by peer");
		}

		assertThat(received).containsExactly("1", "2");
	}

	@Test
	@Timeout(2)
	void closingCancelsTheHandlerThreadInsteadOfLeakingIt() throws Exception {
		AtomicBoolean handlerInterrupted = new AtomicBoolean();
		AtomicBoolean handlerCleanedUp = new AtomicBoolean();
		McpServer server = new McpServer().streamOn("hang", (request, notifications) -> {
			notifications.send("started"); // let the caller see the handler is actually running
			while (true) {
				try {
					Thread.sleep(Long.MAX_VALUE); // hang forever unless cancelled
				}
				catch (InterruptedException ex) {
					handlerInterrupted.set(true);
					throw ex;
				}
				finally {
					handlerCleanedUp.set(true);
				}
			}
		});

		try (Source<String> responses = server.call("hang")) {
			assertThat(responses.receiveNext()).isTrue();
			assertThat(responses.next()).isEqualTo("started");
			// cancel while the handler is definitely blocked in sleep(); close() below
			// blocks until the handler thread has actually stopped, so both flags are
			// guaranteed set once it returns
		}

		assertThat(handlerInterrupted).as("close() interrupted the blocked handler thread").isTrue();
		assertThat(handlerCleanedUp).as("the handler's own cleanup ran before close() returned").isTrue();
	}

	// -- one server, both kinds of handler --------------------------------------

	@Test
	@Timeout(2)
	void aServerCanRegisterBothResponseAndStreamHandlersForDifferentRequests() throws Exception {
		McpServer server = new McpServer()
				.respondTo("hello", request -> request.toUpperCase())
				.streamOn("stream:chunk", (request, notifications) -> {
					notifications.send(request + "-1");
					notifications.send(request + "-2");
					return request.toUpperCase();
				});

		assertThat(drain(server.call("hello"))).containsExactly("HELLO");
		assertThat(drain(server.call("stream:chunk")))
				.containsExactly("stream:chunk-1", "stream:chunk-2", "STREAM:CHUNK");
	}

	// -- fan-out to independent subscribers -----------------------------------

	@Test
	@Timeout(2)
	void broadcast_publishesToEverySubscriberAndCancellingOneLeavesTheRestUnaffected() throws Exception {
		MultiSink<String> multiSink = MultiSink.of(List.of());
		Sink<String> pubSubSink = multiSink.start();
		CountDownLatch registered = new CountDownLatch(2);
		McpServer server = new McpServer().streamOn("topic", (topic, updates) -> {
			multiSink.registerListener(updates);
			registered.countDown();
			while (true) {
				try {
					Thread.sleep(Long.MAX_VALUE); // stays subscribed until the runtime cancels
				}
				finally {
					multiSink.unregisterListener(updates);
				}
			}
		});

		Source<String> a = server.call("topic");
		Source<String> b = server.call("topic");
		registered.await(); // subscribing is async: wait for both handler threads to register

		pubSubSink.send("first update");
		assertThat(a.receiveNext()).isTrue();
		assertThat(a.next()).isEqualTo("first update");
		assertThat(b.receiveNext()).isTrue();
		assertThat(b.next()).isEqualTo("first update");

		a.close(); // "a" cancels; close() is synchronous, so its unsubscribe has already run

		try (b) {
			pubSubSink.send("second update");
			assertThat(b.receiveNext()).isTrue();
			assertThat(b.next()).isEqualTo("second update");
		}
		finally {
			pubSubSink.complete();
		}
	}

	@Test
	@Timeout(2)
	void broadcast_aSubscriberThatCanNoLongerAcceptUpdatesDoesNotStopDeliveryToTheRest() throws Exception {
		BlockingQueueExchange<String> gone = new BlockingQueueExchange<>();
		gone.complete(); // this subscriber already hung up

		BlockingQueueExchange<String> alive = new BlockingQueueExchange<>();

		MultiSink<String> multiSink = MultiSink.of(List.of(gone, alive));
		Sink<String> pubSubSink = multiSink.start();
		pubSubSink.send("list changed");

		assertThat(alive.receiveNext()).isTrue();
		assertThat(alive.next()).isEqualTo("list changed");
		pubSubSink.complete();
	}

}
