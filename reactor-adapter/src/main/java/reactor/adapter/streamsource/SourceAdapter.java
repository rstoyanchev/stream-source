package reactor.adapter.streamsource;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Source;
import io.spring.streamsource.flow.FlowSourceAdapter;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Variant of {@link FlowSourceAdapter} for Reactor, adapting a {@link Source} to a
 * {@link Flux}, and a Reactive Streams {@link Publisher} to a {@code Source}.
 *
 * <p>Semantics are those of {@link FlowSourceAdapter}. The difference is that this adapter is
 * implemented with Reactor operators, so Reactor context propagation applies to both
 * directions when {@code io.micrometer:context-propagation} is on the classpath:
 * <ul>
 * <li>{@link #toFlux(Source)}: with
 * {@link reactor.core.publisher.Hooks#enableAutomaticContextPropagation() automatic
 * context propagation} enabled, ThreadLocals are restored from the subscriber
 * {@link reactor.util.context.Context} around each receive, on the thread that runs
 * the receive loop.
 * <li>{@link #toSource(Publisher)}: the ThreadLocals of the consumer thread, at the time
 * of the first receive, are captured into the {@code Context} of the subscription with
 * {@link Flux#contextCapture()}.
 * </ul>
 */
public abstract class SourceAdapter {

	private static final @Nullable Executor defaultExecutor = createDefaultExecutor();


	private SourceAdapter() {
	}


	/**
	 * Adapt a {@link Source} to a {@link Flux} that receives from it on a virtual
	 * thread.
	 * <p>Requires Java 21+, use {@link #toFlux(Source, Executor)} on Java &lt; 21.
	 * @param source the source to receive from
	 * @return a Flux that emits the items of the Source
	 * @param <T> the type of items
	 * @throws IllegalStateException if virtual threads are not supported
	 * @see FlowSourceAdapter#toPublisher(Source)
	 */
	public static <T> Flux<T> toFlux(Source<T> source) {
		Executor executor = defaultExecutor;
		if (executor == null) {
			throw new IllegalStateException("Virtual threads require Java 21+, use " +
					"toFlux(Source, Executor) instead");
		}
		return toFlux(source, executor);
	}

	/**
	 * Variant of {@link #toFlux(Source)} with the {@code Executor} to receive on.
	 * The receive loop blocks, so this must be an executor where blocking is allowed,
	 * compatible with Java &lt; 21. The caller remains responsible for its lifecycle.
	 * @param source the source to receive from
	 * @param executor the executor that runs the receive loop
	 * @return a Flux that emits the items of the Source
	 * @param <T> the type of items
	 * @see FlowSourceAdapter#toPublisher(Source, Executor)
	 */
	public static <T> Flux<T> toFlux(Source<T> source, Executor executor) {
		AtomicBoolean subscribed = new AtomicBoolean();
		return Flux.using(
				() -> {
					if (!subscribed.compareAndSet(false, true)) {
						throw new IllegalStateException("A Source supports a single subscriber");
					}
					return source;
				},
				// One tick per requested item, each turned into a receive by handle, which
				// (unlike generate) restores ThreadLocals in automatic context propagation
				s -> Flux.<Boolean>generate(sink -> sink.next(Boolean.TRUE))
						.<T>handle((tick, sink) -> {
							try {
								if (s.receiveNext()) {
									sink.next(s.next());
								}
								else {
									sink.complete();
								}
							}
							catch (InterruptedException ex) {
								Thread.currentThread().interrupt();
								sink.error(ex);
							}
							catch (IOException ex) {
								sink.error(ex);
							}
						})
						// Trampoline worker, so the receive loop is serialized
						.subscribeOn(Schedulers.fromExecutor(executor, true)),
				// Eager cleanup: cancelling closes the Source, unblocking a pending receive
				Source::close);
	}

	/**
	 * Adapt a Reactive Streams {@link Publisher} to a {@link Source} that buffers up to
	 * {@link BlockingQueueExchange#DEFAULT_CAPACITY} items ahead of the consumer.
	 * @param publisher the publisher to subscribe to
	 * @return a Source over the items of the publisher
	 * @param <T> the type of items
	 * @see FlowSourceAdapter#toSource(java.util.concurrent.Flow.Publisher)
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher) {
		return toSource(publisher, BlockingQueueExchange.DEFAULT_CAPACITY);
	}

	/**
	 * Variant of {@link #toSource(Publisher)} with the number of items to buffer ahead
	 * of the consumer, which is also the demand the publisher is given.
	 * @param publisher the publisher to subscribe to
	 * @param capacity the number of items to buffer, and to request
	 * @return a Source over the items of the publisher
	 * @param <T> the type of items
	 * @see FlowSourceAdapter#toSource(java.util.concurrent.Flow.Publisher, int)
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher, int capacity) {
		// Subscribed on the first receive, so this captures the consumer thread's ThreadLocals
		return FlowSourceAdapter.toSource(
				JdkFlowAdapter.publisherToFlowPublisher(Flux.from(publisher).contextCapture()), capacity);
	}

	private static @Nullable Executor createDefaultExecutor() {
		try {
			return new ReactorVirtualThreadDelegate().newVirtualThreadPerTaskExecutor();
		}
		catch (UnsupportedOperationException ex) {
			return null;
		}
	}

}
