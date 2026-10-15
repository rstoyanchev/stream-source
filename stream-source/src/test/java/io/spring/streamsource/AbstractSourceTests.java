package io.spring.streamsource;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AbstractSource}.
 *
 * <p>Tests in this class are aligned with {@link BlockingQueueExchangeTests} for
 * receiving logic. Methods names and assertion details may vary slightly, but
 * test logic and expectations should be the same. New tests here should be
 * added to both tests classes to maintain the equivalence.
 */
@SuppressWarnings("resource")
class AbstractSourceTests {


	@Test
	void receive() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");
	}

	@Test
	void receiveToEnd() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	void receiveToException() throws IOException, InterruptedException {
		Source<String> source = TestSource.withItems("item 1", "item 2").failure(new IllegalStateException("boom"));

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThatIllegalStateException().isThrownBy(source::receiveNext).withMessage("boom");
	}

	@Test
	void receiveToIOException() {
		IOException ex = new IOException("simulated I/O error");
		Source<String> source = TestSource.withFailure(ex);

		assertThatIOException().isThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveToInterruptedException() {
		InterruptedException ex = new InterruptedException("simulated interrupt");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveToRuntimeException() {
		RuntimeException ex = new RuntimeException("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveToError() {
		Error ex = new Error("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveAfterCloseRethrows() {
		IOException ex = new IOException("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveMultipleTimesSkips() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	void nextRequiresReceive() {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThatIllegalStateException().isThrownBy(source::next).withMessage("No received item");
	}

	@Test
	void close() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");
		source.close();

		assertThat(source.receiveNext()).isFalse();
	}

}
