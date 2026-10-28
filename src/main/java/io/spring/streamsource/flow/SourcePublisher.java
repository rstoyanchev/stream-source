package io.spring.streamsource.flow;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Publisher} that receives from a {@link Source} on a background thread,
 * turning the blocking receive loop into Reactive Streams signals.
 *
 * <p>See {@link FlowSourceAdapter} for more details.
 *
 * @param <T> the type of items received
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
		ReceiveTask<T> task = new ReceiveTask<>(this.source, subscriber);
		try {
			subscriber.onSubscribe(task);
		}
		catch (Throwable ex) {
			// §2.13: the subscription is cancelled, and no task thread will close the Source
			closeSource();
			throw ex;
		}
		if (task.isCancelled()) {
			// Canceled from onSubscribe
			task.signalPendingError();
			closeSource();
			return;
		}
		try {
			this.executor.execute(task);
		}
		catch (RejectedExecutionException ex) {
			task.handleRejectedExecution(ex);
		}
	}

	/**
	 * Close the Source on the Executor, subscribing thread may be non-blocking.
	 */
	private void closeSource() {
		try {
			this.executor.execute(this.source::close);
		}
		catch (RejectedExecutionException ex) {
			this.source.close();
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + this.source;
	}


	/**
	 * The receive task and the {@link Subscription} handed to the {@code Subscriber}.
	 * Items are emitted from this task only, serializing the Publisher signals.
	 */
	private static final class ReceiveTask<T> implements Runnable, Subscription {

		private static final Logger logger = LoggerFactory.getLogger(SourcePublisher.class);

		private static final int IDLE = 0;

		private static final int RECEIVING = 1;

		private static final int INTERRUPTING = 2;

		private static final int INTERRUPTED = 3;

		private final Source<T> source;

		private final Subscriber<? super T> actual;

		/** Outstanding demand, {@code Long.MAX_VALUE} for unbounded. */
		private final AtomicLong requested = new AtomicLong();

		/** Holds a {@code request(n)} error to be signaled from the receive thread. */
		private final AtomicReference<@Nullable Throwable> pendingError = new AtomicReference<>();

		private volatile boolean cancelled;

		/** Set in {@link #run()} to allow waking or interrupting the receive thread. */
		private volatile @Nullable Thread receiveThread;

		/** State variable manage interrupting the receive thread only while blocked on {@link Source#receiveNext()}. */
		private final AtomicInteger phase = new AtomicInteger(IDLE);

		ReceiveTask(Source<T> source, Subscriber<? super T> actual) {
			this.source = source;
			this.actual = actual;
		}

		/**
		 * Whether the Subscription side is cancelled.
		 */
		public boolean isCancelled() {
			return this.cancelled;
		}

		/**
		 *
		 */
		public void handleRejectedExecution(RejectedExecutionException ex) {
			this.source.close();
			if (!this.cancelled) {
				this.actual.onError(ex);
			}
			else {
				signalPendingError();
			}
		}

		@Override
		public void run() {
			this.receiveThread = Thread.currentThread();
			boolean interrupted = false;
			try {
				while (awaitDemand()) {
					if (!receiveNext()) {
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
				// receiveNext clears the interrupt from cancel()
				// interrupts from awaitDemand to be restored after close()
				interrupted = !this.cancelled;
				error(ex);
			}
			catch (Throwable ex) {
				error(ex);
			}
			finally {
				this.receiveThread = null;
				this.source.close();
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
		}

		/**
		 * Park until there is demand to receive, and return whether to carry on.
		 * If {@link LockSupport#unpark(Thread)} is called in {@code request(n)}
		 * just before this in a race, a permit is left, and therefore not lost.
		 */
		private boolean awaitDemand() throws InterruptedException {
			while (!this.cancelled && this.requested.get() == 0) {
				LockSupport.park();
				// cancel() interrupts only receiveNext
				// any interrupt here is different (e.g. executor shutting down),
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
			return !this.cancelled;
		}

		private boolean receiveNext() throws IOException, InterruptedException {
			this.phase.set(RECEIVING);
			try {
				// Check cancelled after setting the phase: cancel() that saw IDLE would not interrupt
				return (!this.cancelled && this.source.receiveNext());
			}
			finally {
				if (!this.phase.compareAndSet(RECEIVING, IDLE)) {
					// cancel() got there first: wait for its interrupt to land, then
					// clear it, so that it cannot reach the subscriber, close(), or
					// whatever the executor runs next on this thread
					while (this.phase.get() == INTERRUPTING) {
						Thread.yield();
					}
					Thread.interrupted();
					this.phase.set(IDLE);
				}
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
					// Source interrupted by cancel() and handling an interruption related exception, or
					// cancellation racing with a Source error
					logger.debug("cancelled, discarding {}", ex.toString());
				}
			}
			else {
				this.actual.onError(ex);
			}
		}

		void signalPendingError() {
			Throwable ex = this.pendingError.getAndSet(null);
			if (ex != null) {
				this.actual.onError(ex);
			}
		}

		// Subscription methods, called by reactive subscriber on any thread

		@Override
		public void request(long n) {
			if (n <= 0) {
				// Cancel, and let the receive thread signal the error, so that it is not
				// concurrent with an onNext in progress.
				if (!this.cancelled) {
					this.pendingError.compareAndSet(null, new IllegalArgumentException(
							"request amount must be positive, but it was " + n));
					cancel();
				}
				return;
			}
			this.requested.getAndUpdate(r -> {
				long sum = r + n;
				return (sum < 0 ? Long.MAX_VALUE : sum);
			});
			unpark(this.receiveThread);
		}

		/**
		 * {@inheritDoc}
		 * <p>Interrupts the receive thread if it is blocked in receiveNext,
		 * or unparks it if in awaitDemand(). This avoids blocking, and allows
		 * the receive thread instead to close the {@code Source} when it exits.
		 */
		@Override
		public void cancel() {
			if (this.cancelled) {
				return;
			}
			this.cancelled = true;
			Thread thread = this.receiveThread;
			if (thread == null) {
				return;
			}
			if (this.phase.compareAndSet(RECEIVING, INTERRUPTING)) {
				try {
					thread.interrupt();
				}
				finally {
					this.phase.set(INTERRUPTED);
				}
			}
			else {
				unpark(thread);
			}
		}

		/**
		 * Unpark the receive thread unless we are on it in since it will check
		 * for demand and cancellation before it parking.
		 */
		private static void unpark(@Nullable Thread thread) {
			if (thread != null && thread != Thread.currentThread()) {
				LockSupport.unpark(thread);
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
