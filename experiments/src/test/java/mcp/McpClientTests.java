package mcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class McpClientTests {

	// -- call(String): request/response ---------------------------------------

	@Test
	@Timeout(2)
	void callReturnsTheLastItemTheNetworkSends() throws Exception {
		McpClient client = McpClient.withResponse((request, sink) -> {
			sink.send(request.toUpperCase());
			sink.complete();
		});

		assertThat(client.call("hello")).isEqualTo("HELLO");
		assertThat(client.call("world")).isEqualTo("WORLD");
	}

	@Test
	@Timeout(2)
	void callDiscardsNotificationsAndKeepsTheLastItemAsTheResponse() throws Exception {
		McpClient client = McpClient.withResponse((request, sink) -> {
			sink.send("n1");
			sink.send("n2");
			sink.send("final");
			sink.complete();
		});

		assertThat(client.call("chunk")).isEqualTo("final");
	}

	@Test
	@Timeout(2)
	void callPropagatesAFailureFromTheNetworkInsteadOfAResultFlag() {
		McpClient client = McpClient.withResponse((request, sink) ->
				sink.completeExceptionally(new IllegalStateException("handler failed")));

		assertThatThrownBy(() -> client.call("boom"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("handler failed");
	}

	// -- stream(String): the raw Source, undemultiplexed -----------------------

	@Test
	@Timeout(2)
	void streamReturnsTheNetworksSourceUnmodified() throws Exception {
		McpClient client = McpClient.withResponse((request, sink) -> {
			sink.send(request + "-1");
			sink.send(request + "-2");
			sink.send(request + "-3");
			sink.complete();
		});

		List<String> received = new ArrayList<>();
		try (Source<String> responses = client.stream("chunk")) {
			while (responses.receiveNext()) {
				received.add(responses.next());
			}
		}

		assertThat(received).containsExactly("chunk-1", "chunk-2", "chunk-3");
	}

	@Test
	@Timeout(2)
	void streamPropagatesAFailureFromTheNetworkInsteadOfHanging() throws Exception {
		McpClient client = McpClient.withResponse((request, sink) -> {
			sink.send("1");
			sink.send("2");
			sink.completeExceptionally(new IOException("connection reset by peer"));
		});

		List<String> received = new ArrayList<>();
		try (Source<String> responses = client.stream("chunk")) {
			assertThatThrownBy(() -> {
				while (responses.receiveNext()) {
					received.add(responses.next());
				}
			}).isInstanceOf(IOException.class).hasMessage("connection reset by peer");
		}

		assertThat(received).containsExactly("1", "2");
	}

	// -- call(String, Sink): notifications side channel -------------------------

	/**
	 * Demonstrates the hang documented on {@link McpClient#call(String, Sink)}: if
	 * whatever should be draining {@code notifications} is gone, {@code send()} has
	 * nowhere to put the next item and simply never returns -- there is no shared
	 * lifecycle between this call and its side channel's reader, so the call has no
	 * way to notice and give up. {@code notifications} here is never read from at all,
	 * standing in for a reader that has already died; once its bounded queue fills up,
	 * {@code call()} is stuck for good.
	 */
	@Test
	@Timeout(2)
	void callWithASideChannelBlocksForeverWhenTheNotificationReaderIsGone() throws Exception {
		McpClient client = McpClient.withResponse((request, sink) -> {
			for (int i = 0; i < 200; i++) { // more than the queue's fixed capacity of 128
				sink.send("n" + i);
			}
			sink.complete();
		});

		// Nobody ever reads from this Exchange -- standing in for a reader that
		// already died. Its queue is bounded, so send() blocks once it's full.
		BlockingQueueExchange<String> notifications = new BlockingQueueExchange<>();

		AtomicBoolean callReturned = new AtomicBoolean();
		Thread caller = new Thread(() -> {
			try {
				client.call("firehose", notifications);
			}
			catch (Exception ex) {
				// expected once this thread is interrupted below to clean it up;
				// not what this test is checking
			}
			finally {
				callReturned.set(true);
			}
		});
		caller.setDaemon(true);
		caller.start();

		// Plenty of time for 200 sends to complete if nothing were stuck; it
		// never returns on its own, exactly as documented.
		caller.join(500);
		assertThat(callReturned)
				.as("call() never returns: send() is blocked forever with no reader left to drain it")
				.isFalse();

		// Clean up the thread we deliberately left stuck. This can take two
		// interrupts: one for the blocked send(), and one for the
		// completeExceptionally() that follows it in Client#call and blocks the
		// very same way, for the very same reason.
		while (caller.isAlive()) {
			caller.interrupt();
			caller.join(50);
		}
	}

}
