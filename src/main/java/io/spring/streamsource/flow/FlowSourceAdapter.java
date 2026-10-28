package io.spring.streamsource.flow;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;

/**
 * Main class that provides adapters to bridge between {@link Source} and JDK
 * {@link java.util.concurrent.Flow.Publisher}.
 *
 * <p><strong>Source to Publisher Adapter</strong></p>
 *
 * This adapter is a {@link Publisher} that receives from a {@link Source} on a
 * background thread, turning the blocking receive loop into Reactive Streams
 * signals.
 *
 * <p>The receiving thread absorbs the blocking necessary to wait for demand.
 * The thread is parked before each call to {@link Source#receiveNext()},
 * if necessary to await demand, and unparked on the next request signal.
 *
 * <p>A cancel signal works by interrupting the receiving thread and returning
 * without blocking. The {@code Source} is then closed when the receiving task
 * exits. If the {@code Source} does not respond to interruption, the receiving
 * thread remains blocked and the cancellation is not noticed until it is ready
 * to receive again.
 *
 * <p>Note that since the background thread parks before
 * {@link Source#receiveNext()} if there is no demand, {@code onComplete} and
 * {@code onError} signals are only discovered after the subscriber signals
 * non-zero demand.
 *
 * <p><strong>Publisher to Source Adapter</strong></p>
 *
 * This adapter subscribes to a Reactive Streams {@link Publisher}, buffers
 * items in a {@link BlockingQueueExchange}, and exposes those items to a
 * blocking consumer as {@link Source}.
 *
 * <p>The capacity of the {@code BlockingQueueExchange} doubles as the reactive
 * demand window. The initial request equals the {@code capacity}, and each time
 * the consumer takes an item out, another is requested. At any time, the
 * capacity equals the number of buffered items plus the outstanding demand.
 *
 * <p>The use of {@code BlockingQueueExchange} allows published items to be
 * handled without blocking or a separate thread. The blocking consumer blocks
 * on its own thread if necessary to receive the next item.
 */
public abstract class FlowSourceAdapter {

	private static final @Nullable Executor defaultExecutor = createDefaultExecutor();


	private FlowSourceAdapter() {
	}


	/**
	 * Adapt a {@link Source} to a {@link Publisher}.
	 * <p>Requires Java 21+, use {@link #toPublisher(Source, Executor)} on Java &lt; 21.
	 * @param source the source to receive from
	 * @return a Publisher that emits the items received from the Source
	 * @param <T> the type of items received
	 * @throws IllegalStateException if virtual threads are not supported
	 */
	public static <T> Publisher<T> toPublisher(Source<T> source) {
		Executor executor = defaultExecutor;
		if (executor == null) {
			throw new IllegalStateException("Virtual threads require Java 21+, use " +
					"toPublisher(Source, Executor) instead");
		}
		return toPublisher(source, executor);
	}

	/**
	 * Variant of {@link #toPublisher(Source)} for use with Java &lt; 21.
	 * @param source the source to receive from
	 * @param executor the executor to run the receiving thread on;
	 * the caller is responsible for the lifecycle of the executor
	 * @return a Publisher that emits the items received from the Source
	 * @param <T> the type of items received
	 */
	public static <T> Publisher<T> toPublisher(Source<T> source, Executor executor) {
		return new SourcePublisher<>(source, executor);
	}

	/**
	 * Adapt a {@link Publisher} to a {@link Source} with the capacity set to
	 * {@link BlockingQueueExchange#DEFAULT_CAPACITY}.
	 * @param publisher the publisher to subscribe to and receive from
	 * @return a Source over the items from the publisher
	 * @param <T> the type of items received
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher) {
		return toSource(publisher, BlockingQueueExchange.DEFAULT_CAPACITY);
	}

	/**
	 * Variant of {@link #toSource(Publisher)} with a configurable capacity.
	 * @param publisher the publisher to subscribe to
	 * @param capacity the number of items to buffer, and to request
	 * @return a Source over the items of the publisher
	 * @param <T> the type of items
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher, int capacity) {
		return new PublisherSource<>(publisher, capacity);
	}


	private static @Nullable Executor createDefaultExecutor() {
		try {
			return new FlowVirtualThreadDelegate().newVirtualThreadPerTaskExecutor();
		}
		catch (UnsupportedOperationException ex) {
			return null;
		}
	}

}
