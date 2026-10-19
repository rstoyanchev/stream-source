package io.spring.streamsource.flow;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import io.spring.streamsource.AbstractSource;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;

/**
 * Reactive Streams TCK for {@link SourcePublisher}.
 */
public class SourcePublisherTckTests extends FlowPublisherVerification<Long> {

	public SourcePublisherTckTests() {
		super(new TestEnvironment());
	}


	@Override
	public Publisher<Long> createFlowPublisher(long elements) {
		return FlowSourceAdapter.toPublisher(new RangeSource(elements));
	}

	/**
	 * A failure from the Source is discovered by a receive, which needs demand, while
	 * the TCK expects {@code onError} without any. A second subscriber is rejected
	 * without demand, so an already subscribed Publisher is the failed one.
	 */
	@Override
	public Publisher<Long> createFailedFlowPublisher() {
		Publisher<Long> publisher = createFlowPublisher(1);
		publisher.subscribe(new Subscriber<>() {
			@Override
			public void onSubscribe(Subscription subscription) {
				subscription.cancel();
			}
			@Override
			public void onNext(Long item) {
			}
			@Override
			public void onError(Throwable ex) {
			}
			@Override
			public void onComplete() {
			}
		});
		return publisher;
	}


	private static final class RangeSource extends AbstractSource<Long> {

		private final long elements;

		private long next;

		RangeSource(long elements) {
			this.elements = elements;
		}

		@Override
		protected @Nullable Long receiveItem() {
			return (this.next < this.elements ? this.next++ : null);
		}
	}

}
