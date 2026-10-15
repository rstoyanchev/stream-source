package io.spring.streamsource.task;


import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.spring.streamsource.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.shadow.de.siegmar.fastcsv.util.Nullable;

import static io.spring.streamsource.TestSourceUtils.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for {@link ActiveSource}.
 */
public class ActiveSourceTests {


	@Test
	public void produceAndReceive() throws IOException, InterruptedException {
		Producer<String> producer = TestProducer.withItems("a", "b");
		try (Source<String> source = startProducer(producer)) {
			assertThat(toList(source)).containsExactly("a", "b");
		}
	}

	@Test
	public void executorCreatesVirtualThreadsByDefault() throws IOException, InterruptedException {
		AtomicBoolean virtual = new AtomicBoolean();
		Producer<String> producer = TestProducer.withProduceStep(sink -> {
			virtual.set(Thread.currentThread().isVirtual());
			sink.send("a");
			sink.send("b");
		});

		try (Source<String> source = startProducer(producer)) {
			assertThat(toList(source)).containsExactly("a", "b");
		}

		assertThat(virtual).isTrue();
	}

	@Test
	public void executorFixedThreadPool() throws IOException, InterruptedException {
		try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
			AtomicBoolean virtual = new AtomicBoolean(true);
			Producer<String> producer = TestProducer.withProduceStep(sink -> {
				virtual.set(Thread.currentThread().isVirtual());
				sink.send("a");
				sink.send("b");
			});

			try (Source<String> source = startProducer(producer, executor)) {
				assertThat(toList(source)).containsExactly("a", "b");
			}

			assertThat(virtual).isFalse();
		}
	}

	@Test
	public void closeBeforeProducerStartsDoesNotHang() throws Exception {
		try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
			CountDownLatch latch = new CountDownLatch(1);
			executor.submit(() -> {
				latch.await(); // block the single thread, further tasks cannot start
				return null;
			});

			AtomicBoolean produced = new AtomicBoolean();
			Producer<String> producer = TestProducer.withProduceStep(sink -> produced.set(true));
			Source<String> source = startProducer(producer, executor);

			assertTimeoutPreemptively(Duration.ofSeconds(5), source::close);
			latch.countDown();

			executor.shutdown();
			assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
			assertThat(produced).isFalse();
		}
	}


	private static <T> Source<T> startProducer(Producer<T> producer) {
		return startProducer(producer, null);
	}

	private static <T> Source<T> startProducer(Producer<T> producer, @Nullable ExecutorService executor) {
		ActiveSource<T> activeSource = (executor != null ?
				ActiveSource.fromProducer(producer, executor) : ActiveSource.fromProducer(producer));
		return activeSource.start();
	}

}
