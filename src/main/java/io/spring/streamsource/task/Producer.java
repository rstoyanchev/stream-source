package io.spring.streamsource.task;

import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;

/**
 * Component that produces items pushed into a {@link Sink}.
 * @param <T> the type of items produced
 */
@FunctionalInterface
public interface Producer<T> {

	/**
	 * Produce items into the given Sink until there are no more or the Sink is closed.
	 * @param sink the Sink to push items into
	 */
	void produce(Sink<T> sink);


	/**
	 * Create a {@code Producer} that receives from a given {@code Source}.
	 * @param source the source to receive from
	 * @return the producer instance
	 * @param <T> the type of items received and produced
	 */
	static <T> Producer<T> fromSource(Source<T> source) {
		return new SourceProducerAdapter<>(source);
	}

}
