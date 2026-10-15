package reactor.adapter.streamsource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.context.ContextRegistry;
import io.spring.streamsource.AbstractSource;
import io.spring.streamsource.Source;
import io.spring.streamsource.flow.FlowSourceAdapter;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SourceAdapter}, whose semantics are covered in depth
 * by the {@link FlowSourceAdapter} tests.
 */
public class SourceAdapterTests {

	private static final String KEY = "test.key";

	private static final ThreadLocal<String> THREAD_LOCAL = new ThreadLocal<>();


	@AfterEach
	public void cleanup() {
		Hooks.disableAutomaticContextPropagation();
		ContextRegistry.getInstance().removeThreadLocalAccessor(KEY);
		THREAD_LOCAL.remove();
	}

	@Test
	public void toFluxEmitsItemsAndCompletes() {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		StepVerifier.create(SourceAdapter.toFlux(source))
				.expectNext("a", "b", "c")
				.verifyComplete();
	}

	@Test
	public void toFluxSignalsError() {
		IOException exception = new IOException("boom");
		Source<String> source = new AbstractSource<>() {
			@Override
			protected String receiveItem() throws IOException {
				throw exception;
			}
		};
		StepVerifier.create(SourceAdapter.toFlux(source))
				.verifyErrorSatisfies(ex -> assertThat(ex).isSameAs(exception));
	}

	@Test
	public void toFluxCancelClosesSource() throws InterruptedException {
		BlockingSource source = new BlockingSource();
		StepVerifier.create(SourceAdapter.toFlux(source))
				.then(() -> await(source.receiving))
				.thenCancel()
				.verify();
	}

	@Test
	public void toFluxRejectsSecondSubscriber() {
		ListSource<String> source = new ListSource<>("a");
		Flux<String> flux = SourceAdapter.toFlux(source);
		StepVerifier.create(flux)
				.expectNext("a")
				.verifyComplete();
		StepVerifier.create(flux)
				.verifyError(IllegalStateException.class);
	}

	@Test
	public void toFluxHonorsBackpressure() {
		CountingSource source = new CountingSource();
		StepVerifier.create(SourceAdapter.toFlux(source), 0)
				.expectSubscription()
				.thenRequest(1)
				.expectNext(0L)
				.expectNoEvent(Duration.ofMillis(100))
				.thenCancel()
				.verify();
		assertThat(source.receives).isEqualTo(1);
	}

	@Test
	public void toFluxRestoresThreadLocals() {
		registerThreadLocalAccessor();
		Hooks.enableAutomaticContextPropagation();
		AtomicReference<@Nullable String> observed = new AtomicReference<>();
		Source<String> source = new AbstractSource<>() {
			@Override
			protected @Nullable String receiveItem() {
				observed.set(THREAD_LOCAL.get());
				return null;
			}
		};
		StepVerifier.create(SourceAdapter.toFlux(source).contextWrite(Context.of(KEY, "v")))
				.verifyComplete();
		assertThat(observed.get()).isEqualTo("v");
	}

	@Test
	public void toSourceCapturesThreadLocals() throws IOException, InterruptedException {
		registerThreadLocalAccessor();
		THREAD_LOCAL.set("v");
		Source<String> source = SourceAdapter.toSource(Flux.deferContextual(ctx -> Flux.just(ctx.<String>get(KEY))));
		assertThat(receiveAll(source)).containsExactly("v");
	}

	@Test
	public void toSourceReceivesItems() throws IOException, InterruptedException {
		Source<String> source = SourceAdapter.toSource(Flux.just("a", "b", "c"), 2);
		assertThat(receiveAll(source)).containsExactly("a", "b", "c");
	}

	@Test
	public void toSourceFailsWithError() {
		IOException exception = new IOException("boom");
		Source<String> source = SourceAdapter.toSource(Flux.error(exception));
		assertThatThrownBy(source::receiveNext).isSameAs(exception);
	}

	@Test
	public void roundTrip() throws IOException, InterruptedException {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		try (Source<String> result = SourceAdapter.toSource(SourceAdapter.toFlux(source))) {
			assertThat(receiveAll(result)).containsExactly("a", "b", "c");
		}
	}


	private static void registerThreadLocalAccessor() {
		ContextRegistry.getInstance().registerThreadLocalAccessor(KEY, THREAD_LOCAL::get, THREAD_LOCAL::set, THREAD_LOCAL::remove);
	}

	private static void await(CountDownLatch latch) {
		try {
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}
		catch (InterruptedException ex) {
			throw new IllegalStateException(ex);
		}
	}

	private static <T> List<T> receiveAll(Source<T> source) throws IOException, InterruptedException {
		List<T> items = new ArrayList<>();
		while (source.receiveNext()) {
			items.add(source.next());
		}
		return items;
	}


	private static final class ListSource<T> extends AbstractSource<T> {

		private final Iterator<T> iterator;

		@SafeVarargs
		ListSource(T... items) {
			this.iterator = Arrays.asList(items).iterator();
		}

		@Override
		protected @Nullable T receiveItem() {
			return (this.iterator.hasNext() ? this.iterator.next() : null);
		}
	}


	/**
	 * Unbounded Source that counts its receives.
	 */
	private static final class CountingSource extends AbstractSource<Long> {

		private volatile long receives;

		@Override
		protected Long receiveItem() {
			return this.receives++;
		}
	}


	/**
	 * Source whose receive blocks until it is closed.
	 */
	private static final class BlockingSource extends AbstractSource<String> {

		private final CountDownLatch receiving = new CountDownLatch(1);

		private final CountDownLatch closed = new CountDownLatch(1);

		@Override
		protected @Nullable String receiveItem() throws InterruptedException {
			this.receiving.countDown();
			this.closed.await();
			return null;
		}

		@Override
		public void close() {
			super.close();
			this.closed.countDown();
		}
	}

}
