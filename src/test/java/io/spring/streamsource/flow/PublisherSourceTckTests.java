package io.spring.streamsource.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.TimeUnit;

import io.spring.streamsource.Source;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowSubscriberBlackboxVerification;

/**
 * Reactive Streams TCK for the {@link Subscriber} of {@link PublisherSource}.
 */
public class PublisherSourceTckTests extends FlowSubscriberBlackboxVerification<Integer> {

	public PublisherSourceTckTests() {
		super(new TestEnvironment());
	}


	/**
	 * Capture the subscriber that the Source subscribes with, from a consumer thread
	 * that keeps receiving so that demand is replenished, as it would be in use.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Subscriber<Integer> createFlowSubscriber() {
		CompletableFuture<Subscriber<? super Integer>> subscriber = new CompletableFuture<>();
		Publisher<Integer> publisher = subscriber::complete;
		Source<Integer> source = FlowSourceAdapter.toSource(publisher);
		Thread.ofVirtual().start(() -> {
			try (source) {
				while (source.receiveNext()) {
					source.next();
				}
			}
			catch (Exception ex) {
				// ignore, the TCK checks the signals
			}
		});
		try {
			return (Subscriber<Integer>) subscriber.get(5, TimeUnit.SECONDS);
		}
		catch (Exception ex) {
			throw new IllegalStateException(ex);
		}
	}

	@Override
	public Integer createElement(int element) {
		return element;
	}

}
