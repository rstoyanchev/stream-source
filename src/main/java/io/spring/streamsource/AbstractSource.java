package io.spring.streamsource;

import java.io.IOException;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenient base class for {@link Source} implementations that pre-implements
 * the receiving loop, exception handling, and lifecycle management with
 * subclasses left to implement {@link #receiveItem()}.
 */
public abstract class AbstractSource<T> implements Source<T> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());


	private @Nullable T receivedItem;

	private volatile @Nullable Object closure; // Boolean.TRUE | IOException | RuntimeException


	protected AbstractSource() {
	}


	@Override
	public boolean receiveNext() throws IOException, InterruptedException {
		Object closure = this.closure;
		if (closure != null) {
			throwIfClosedWithError(closure);
			return false;
		}
		logger.debug("receiving next..");
		try {
			T item = receiveItem();
			if (item == null) {
				logger.debug("no more items to receive");
				close();
				return false;
			}
			// (this.receivedItem != null) => discarded item
			logger.debug("received {}", item);
			this.receivedItem = item;
			return true;
		}
		catch (IOException | InterruptedException | RuntimeException | Error ex) {
			closeInternal(ex);
			throw ex;
		}
		catch (Throwable ex) {
			// should never happen, no checked exceptions from receiveItem
			RuntimeException wrapped = wrapException(ex);
			closeInternal(wrapped);
			throw wrapped;
		}
	}

	private static void throwIfClosedWithError(Object closure) throws IOException, InterruptedException {
		if (closure instanceof IOException ex) {
			throw ex;
		}
		if (closure instanceof InterruptedException ex) {
			throw ex;
		}
		if (closure instanceof RuntimeException ex) {
			throw ex;
		}
		if (closure instanceof Error ex) {
			throw ex;
		}
		if (closure instanceof Throwable ex) {
			// should never happen, already wrapped in receiveNext on failure
			throw wrapException(ex);
		}
	}

	private static RuntimeException wrapException(Throwable ex) {
		return new RuntimeException("Source closed with error", ex);
	}

	/**
	 * Implement this method to actually retrieve the next item.
	 * @return the next item, or {@code null} if there are no more items to receive
	 */
	protected abstract @Nullable T receiveItem() throws IOException, InterruptedException;

	@Override
	public T next() {
		T item = this.receivedItem;
		if (item == null) {
			throw new IllegalStateException("No received item");
		}
		this.receivedItem = null;
		return item;
	}

	@Override
	public void close() {
		// (this.receivedItem != null) => discarded item
		closeInternal(Boolean.TRUE);
	}

	private void closeInternal(Object closure) {
		if (this.closure != null) {
			return;
		}
		this.closure = closure;
		if (closure instanceof Throwable ex) {
			logger.debug("closed with error {}", ex.toString());
		}
		else {
			logger.debug("closed without error");
		}
	}

}
