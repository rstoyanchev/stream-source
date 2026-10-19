package io.spring.streamsource.flow;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Publisher} that receives from a pull-style {@link Source} on an
 * {@link Executor}, turning the blocking receive loop into Reactive Streams signals.
 * Created through {@link FlowSourceAdapter#toPublisher(Source)}.
 *
 * <p>The loop thread is where the blocking happens, and demand is what it waits for:
 * with no outstanding {@code request(n)} it parks on a condition rather than calling
 * {@link Source#receiveNext()}, so nothing is received that nobody asked for. Because
 * the loop parks, the executor must be one where blocking is allowed.
 *
 * <p>Waiting for demand also means the end of the stream is not discovered until it is
 * asked for: {@code onComplete} is signalled by the receive that follows the last item,
 * which requires demand like any other.
 *
 * <p>A Source has a single consumer, so this Publisher accepts a single subscriber.
 *
 * @param <T> the type of items received through the Source
 */
final class SourcePublisher<T> implements Publisher<T> {

	private final Source<T> source;

	private final Executor executor;

	private final AtomicBoolean subscribed = new AtomicBoolean();


	SourcePublisher(Source<T> source, Executor executor) {
		this.source = source;
		this.executor = executor;
	}


	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		Objects.requireNonNull(subscriber, "Subscriber must not be null");
		if (!this.subscribed.compareAndSet(false, true)) {
			subscriber.onSubscribe(NoOpSubscription.INSTANCE);
			subscriber.onError(new IllegalStateException(
					"A Source has a single consumer, so " + this + " allows a single subscriber"));
			return;
		}
		ReceiveLoop<T> loop = new ReceiveLoop<>(this.source, subscriber);
		subscriber.onSubscribe(loop);
		if (loop.isCancelled()) {
			// Cancelled from onSubscribe, nothing to run, but an invalid request(n)
			// there still has an error to report, and no loop thread to do it.
			loop.signalPendingError();
			return;
		}
		try {
			this.executor.execute(loop);
		}
		catch (RejectedExecutionException ex) {
			loop.rejected(ex);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + this.source;
	}


	/**
	 * The receive loop, and the {@link Subscription} handed to the subscriber. Items are
	 * emitted from the loop thread only, which is what serializes the signals.
	 */
	private static final class ReceiveLoop<T> implements Subscription, Runnable {

		private static final Logger logger = LoggerFactory.getLogger(SourcePublisher.class);

		private final Source<T> source;

		private final Subscriber<? super T> actual;

		private final ReentrantLock lock = new ReentrantLock();

		private final Condition demandAvailable = this.lock.newCondition();

		/** Outstanding demand, {@code Long.MAX_VALUE} meaning unbounded. */
		private final AtomicLong requested = new AtomicLong();

		/** Error from an invalid {@code request(n)}, signalled by the loop thread. */
		private final AtomicReference<@Nullable Throwable> pendingError = new AtomicReference<>();

		private volatile boolean cancelled;


		ReceiveLoop(Source<T> source, Subscriber<? super T> actual) {
			this.source = source;
			this.actual = actual;
		}


		// Subscriber side, called from any thread

		@Override
		public void request(long n) {
			if (n <= 0) {
				// Cancel, and let the loop thread signal the error, so that it is not
				// concurrent with an onNext in progress.
				if (!this.cancelled) {
					this.pendingError.compareAndSet(null, new IllegalArgumentException(
							"request amount must be positive, but it was " + n));
					cancel();
				}
				return;
			}
			this.lock.lock();
			try {
				this.requested.getAndUpdate(r -> {
					long sum = r + n;
					return (sum < 0 ? Long.MAX_VALUE : sum);
				});
				this.demandAvailable.signal();
			}
			finally {
				this.lock.unlock();
			}
		}

		/**
		 * {@inheritDoc}
		 * <p>Closing the Source is what unblocks a receive that is already in progress,
		 * so that is done here rather than left to the loop thread, which may be parked
		 * in {@link Source#receiveNext()} with no reason to return. It follows that this
		 * blocks for as long as the Source takes to close, and that the Source sees
		 * {@code close()} from the cancelling thread.
		 */
		@Override
		public void cancel() {
			if (this.cancelled) {
				return;
			}
			this.lock.lock();
			try {
				this.cancelled = true;
				this.demandAvailable.signal();
			}
			finally {
				this.lock.unlock();
			}
			this.source.close();
		}

		boolean isCancelled() {
			return this.cancelled;
		}

		void rejected(RejectedExecutionException ex) {
			this.source.close();
			if (!this.cancelled) {
				this.actual.onError(ex);
			}
			else {
				signalPendingError();
			}
		}

		void signalPendingError() {
			Throwable ex = this.pendingError.getAndSet(null);
			if (ex != null) {
				this.actual.onError(ex);
			}
		}


		// Loop side, on the executor thread

		@Override
		public void run() {
			try {
				while (awaitDemand()) {
					if (!this.source.receiveNext()) {
						complete();
						return;
					}
					T item = this.source.next();
					if (this.cancelled) {
						logger.debug("cancelled, discarding {}", item);
						break;
					}
					this.actual.onNext(item);
					produced();
				}
				signalPendingError();
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				error(ex);
			}
			catch (Throwable ex) {
				error(ex);
			}
			finally {
				this.source.close();
			}
		}

		/**
		 * Park until there is demand to receive one more item, and report whether the
		 * loop should carry on.
		 */
		private boolean awaitDemand() throws InterruptedException {
			this.lock.lock();
			try {
				while (!this.cancelled && this.requested.get() == 0) {
					this.demandAvailable.await();
				}
				return !this.cancelled;
			}
			finally {
				this.lock.unlock();
			}
		}

		private void produced() {
			this.requested.getAndUpdate(r -> (r == Long.MAX_VALUE ? r : r - 1));
		}

		private void complete() {
			if (!this.cancelled) {
				this.actual.onComplete();
			}
			else {
				signalPendingError();
			}
		}

		private void error(Throwable ex) {
			if (this.cancelled) {
				if (this.pendingError.get() != null) {
					signalPendingError();
				}
				else {
					// Nobody left to report to, and typically the Source unwinding from
					// the close() that cancel() did, like the SocketException from an
					// interrupted read.
					logger.debug("cancelled, discarding {}", ex.toString());
				}
			}
			else {
				this.actual.onError(ex);
			}
		}
	}


	private static final class NoOpSubscription implements Subscription {

		static final NoOpSubscription INSTANCE = new NoOpSubscription();

		@Override
		public void request(long n) {
		}

		@Override
		public void cancel() {
		}
	}

}
