package mcp;

import java.io.IOException;

import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;

public final class McpClient {

	private final NetworkClient networkClient;

	public McpClient(NetworkClient networkClient) {
		this.networkClient = networkClient;
	}

	/**
	 * Request/response: drain the call to its natural end and keep the last item
	 * as the response, silently discarding notifications sent before it.
	 */
	public String call(String request) throws IOException, InterruptedException {
		return call(request, DISCARDING_SINK);
	}

	/**
	 * The raw call, for a caller that wants the {@link Source} itself instead of
	 * the request/response view -- pulling it directly, one item at a time, and
	 * deciding for itself what's a notification and what's the response (the
	 * last item before it ends). This is what the current design leaves you with
	 * when {@link #call(String, Sink)}'s inline push isn't good enough either: it
	 * hides nothing and coordinates nothing, which is both its honesty and its
	 * limitation -- there is no richer combinator here to split one {@code
	 * Source} into an independently-paced response and notification stream, only
	 * this and doing the demultiplexing by hand.
	 */
	public Source<String> stream(String request) {
		return this.networkClient.call(request);
	}

	/**
	 * Request/response with a side channel: pushes everything the server sends
	 * before the response into {@code notifications} as it arrives, then returns
	 * the response once the call ends naturally. Handing {@code notifications} a
	 * {@link Sink} that is itself the write end of an {@link
	 * io.spring.streamsource.Exchange} -- a {@code
	 * BlockingQueueExchange}, say -- lets the caller keep the read end for a
	 * separate thread of its own, so notifications get processed independently of
	 * whatever this call is doing to get the response.
	 *
	 * <h2>Why this is a compromise</h2>
	 * This is still built by pulling the one {@link Source} the transport gives
	 * us on this thread -- there is no other way to tell "a notification" from
	 * "the response" than to look at what comes after it. Two consequences follow
	 * from that, and neither goes away just because the caller moved its own
	 * processing to another thread:
	 * Nothing ties this call's lifetime to whatever is reading from the other
	 * end of {@code notifications}. If that reader dies, this call eventually
	 * blocks forever on {@code send()}; if this call fails, the only thing the
	 * reader is told is {@link Sink#completeExceptionally} -- there's no
	 * cancellation flowing back the other way.
	 *
	 * <h2>With structured concurrency</h2>
	 * A {@code StructuredTaskScope} would let the two halves be siblings instead
	 * of one driving the other through a hand-off {@code Sink}: fork the pull
	 * loop that reads from the server's {@code Source} as one subtask, fork the
	 * caller's notification handling as another, and join the scope with a
	 * policy that fits -- "if either fails, cancel the rest" being the natural
	 * one here. Closing the scope would then guarantee both are done, one way or
	 * another, before this method returns: no thread the caller forgot to join,
	 * no {@code send()} left blocking forever because the reader is gone. That
	 * structural guarantee, not just the extra thread, is what this hand-written
	 * version is missing.
	 */
	public String call(String request, Sink<String> notifications) throws IOException, InterruptedException {
		try (Source<String> responses = this.networkClient.call(request)) {
			String response = null;
			while (responses.receiveNext()) {
				if (response != null) {
					notifications.send(response);
				}
				response = responses.next();
			}
			if (response == null) {
				throw new IOException("server closed the stream without a response");
			}
			notifications.complete();
			return response;
		}
		catch (IOException | InterruptedException | RuntimeException ex) {
			notifications.completeExceptionally(ex);
			throw ex;
		}
	}


	public static McpClient withResponse(NetworkClient.MockResponseHandler consumer) {
		return new McpClient(NetworkClient.createWithResponse(consumer));
	}


	private static final Sink<String> DISCARDING_SINK = new Sink<>() {

		@Override
		public void send(String item) {
		}

		@Override
		public void complete() {
		}

		@Override
		public void completeExceptionally(Throwable cause) {
		}
	};
}
