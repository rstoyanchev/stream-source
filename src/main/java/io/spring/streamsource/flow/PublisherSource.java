package io.spring.streamsource.flow;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.spring.streamsource.AbstractSource;
import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.SinkCompleteException;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;

/**
 * A {@link Source} that subscribes to a Reactive Streams {@link Publisher},
 * buffers items in a {@link BlockingQueueExchange}, and exposes the items to
 * a blocking consumer.
 *
 * <p>See {@link FlowSourceAdapter} for more details.
 *
 * @param <T> the type of items published
 */
final class PublisherSource<T> extends AbstractSource<T> {

	private final Publisher<T> publisher;

	private final int capacity;

	private final BlockingQueueExchange<T> exchange;

	private final ExchangeSubscriber subscriber = new ExchangeSubscriber();

	private boolean subscribed;


	PublisherSource(Publisher<T> publisher, int capacity) {
		this.publisher = publisher;
		this.capacity = capacity;
		this.exchange = new BlockingQueueExchange<>(capacity);
	}


	@Override
	protected @Nullable T receiveItem() throws IOException, InterruptedException {
		if (!this.subscribed) {
			// Subscribe on the first receive, so that creating a Source starts nothing
			logger.debug("subscribing to {}", this.publisher);
			this.subscribed = true;
			this.publisher.subscribe(this.subscriber);
		}
		if (!this.exchange.receiveNext()) {
			return null;
		}
		T item = this.exchange.next();
		// Give the publisher back the slot the item freed
		this.subscriber.request(1);
		return item;
	}

	@Override
	protected void handleClose(@Nullable Throwable failure) {
		this.subscriber.cancel(); // stops the inflow, also if not yet subscribed
		this.exchange.close(); // discards buffered items
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + this.publisher;
	}


	/**
	 * {@code Subscriber} that buffers items in a {@link BlockingQueueExchange}.
	 */
	private final class ExchangeSubscriber implements Subscriber<T> {

		/** Marks the subscription as done (cancelled or terminated), including before it arrived. */
		private static final Subscription CANCELLED = new Subscription() {
			@Override
			public void request(long n) {
			}
			@Override
			public void cancel() {
			}
		};

		private final AtomicReference<@Nullable Subscription> subscription = new AtomicReference<>();

		/** Items requested but not yet received. */
		private final AtomicLong outstanding = new AtomicLong();

		@Override
		public void onSubscribe(Subscription subscription) {
			Objects.requireNonNull(subscription, "Subscription must not be null");
			if (this.subscription.compareAndSet(null, subscription)) {
				this.outstanding.addAndGet(capacity);
				subscription.request(capacity);
			}
			else {
				// §2.5 for a second subscription, or the Source was closed already
				subscription.cancel();
			}
		}

		@Override
		public void onNext(T item) {
			Objects.requireNonNull(item, "Item must not be null");
			if (this.subscription.get() == CANCELLED) {
				logger.debug("discarding item received after close: {}", item);
				return;
			}
			if (this.outstanding.decrementAndGet() < 0) {
				// reachable if the publisher sent more than asked
				demandExceeded();
				return;
			}
			try {
				if (!exchange.offer(item)) {
					// demand accounting above should make this unreachable, but
					// protect against a publisher that sends more
					demandExceeded();
				}
			}
			catch (SinkCompleteException ex) {
				// consumer closed the Source
				logger.debug("discarding item received after close: {}", item);
				cancel();
			}
		}

		private void demandExceeded() {
			cancel();
			exchange.completeExceptionally(new IllegalStateException(
					"Publisher exceeded its demand of " + capacity + " items: " + publisher));
		}

		@Override
		public void onError(Throwable ex) {
			Objects.requireNonNull(ex, "Throwable must not be null");
			if (terminate()) {
				exchange.completeExceptionally(ex);
			}
			else {
				logger.debug("discarding error received after close: {}", ex.toString());
			}
		}

		@Override
		public void onComplete() {
			if (terminate()) {
				exchange.complete();
			}
		}

		/**
		 * Mark the subscription as done, and report whether it was active.
		 */
		private boolean terminate() {
			return (this.subscription.getAndSet(CANCELLED) != CANCELLED);
		}

		// Methods for use from the receiving loop to signal to the upstream Publisher

		void request(long n) {
			Subscription subscription = this.subscription.get();
			if (subscription != null && subscription != CANCELLED) {
				this.outstanding.addAndGet(n);
				subscription.request(n);
			}
		}

		void cancel() {
			Subscription subscription = this.subscription.getAndSet(CANCELLED);
			if (subscription != null && subscription != CANCELLED) {
				subscription.cancel();
			}
		}
	}

}
