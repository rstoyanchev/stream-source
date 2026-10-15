package io.spring.streamsource;

import java.io.IOException;

/**
 * Contract to send items to a Sink.
 * @param <T> the types of items sent to the Sink
 */
public interface Sink<T> {

	/**
	 * Send the next item, blocking if necessary until the Sink can accept it.
	 * @throws SinkCompleteException if the Sink was already completed or
	 * otherwise, can no longer send items
	 * @throws InterruptedException if send was interrupted the Sink should
	 * {@link #completeExceptionally} to signal that sending is complete due to
	 * interruption
	 */
	void send(T item) throws IOException, InterruptedException;

	/**
	 * Call this to complete the Sink normally, signaling there are no more
	 * items to send.
	 */
	void complete();

	/**
	 * Call this to complete the Sink with an error, signaling no more items
	 * will be sent due to the error.
	 */
	void completeExceptionally(Throwable cause);

}
