package io.spring.streamsource.task;

import io.spring.streamsource.Sink;

/**
 * Convenient base class for {@link Producer} implementations that implements
 * the produce method, and handles completion and exception handling.
 *
 * <p>To do the actual work, subclasses can implement
 * {@link #produceInternal(Sink)} for the producer loop.
 */
public abstract class AbstractProducer<T> implements Producer<T> {

	/**
	 * Protected constructor for subclasses.
	 */
	protected AbstractProducer() {
	}


	@Override
	public void produce(Sink<T> sink) {
		try {
			produceInternal(sink);
			sink.complete();
		}
		catch (Throwable ex) {
			sink.completeExceptionally(ex);
		}
	}

	/**
	 * The main method to implement for the producer loop. The method does not
	 * need to handle Sink completion and can propagate any exception.
	 * @param sink the Sink to push items into
	 */
	protected abstract void produceInternal(Sink<T> sink) throws Throwable;

}
