package io.spring.streamsource;

import java.io.IOException;

/**
 * Contract to receive items from a Source.
 * @param <T> the types of items received from the Source
 */
public interface Source<T> extends AutoCloseable {

	/**
	 * Request to receive the next item from the Source.
	 * <p>A return value of {@code true} guarantees {@link #next()} will return
	 * an item, while {@code false} means the Source won't return more items.
	 * <p>Each call receives a new item, possibly discarding the previous item
	 * if {@link #next()} wasn't called.
	 * @return {@code true} if there is at least one item to receive
	 * @throws IOException if the receive encounters an I/O error
	 * @throws InterruptedException if the current thread is interrupted,
	 * the Source should close
	 */
	boolean receiveNext() throws IOException, InterruptedException;

	/**
	 * Return the next item. Calls to this method must be preceeded by a call to
	 * {@link #receiveNext()} that returns {@code true}, and may be called only
	 * once to retrieve a given item.
	 * @throws IllegalStateException if the call was not preceeded by a call to
	 * {@link #receiveNext()} that returned {@code true}
	 */
	T next();

	/**
	 * Close the Source and discard any received items.
	 */
	@Override
	void close();

}
