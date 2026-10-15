package reactor.adapter.streamsource;

import io.spring.streamsource.AbstractSource;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Flux;

/**
 * Reactive Streams TCK for {@link SourceAdapter#toFlux}.
 */
public class SourceAdapterTckTests extends PublisherVerification<Long> {

	public SourceAdapterTckTests() {
		super(new TestEnvironment());
	}


	@Override
	public Publisher<Long> createPublisher(long elements) {
		return SourceAdapter.toFlux(new RangeSource(elements));
	}

	/**
	 * A failure from the Source is discovered by a receive, which needs demand, while
	 * the TCK expects {@code onError} without any. A second subscriber is rejected
	 * without demand, so an already subscribed Flux is the failed one.
	 */
	@Override
	public Publisher<Long> createFailedPublisher() {
		Flux<Long> flux = SourceAdapter.toFlux(new RangeSource(1));
		flux.subscribe(item -> {}, ex -> {}, () -> {}, subscription -> subscription.cancel());
		return flux;
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
