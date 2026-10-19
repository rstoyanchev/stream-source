package io.spring.streamsource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * A {@link Source} for testing that emits a fixed set of items possibly
 * followed by an exception.
 */
@SuppressWarnings("resource")
public final class TestSource<T> extends AbstractSource<T> {

	private final List<Object> items = new ArrayList<>();

	private Iterator<Object> iterator;


	private TestSource() {
	}


	@SafeVarargs
	public final TestSource<T> items(T... items) {
		this.items.addAll(Arrays.asList(items));
		return this;
	}

	public Source<T> failure(Throwable ex) {
		this.items.add(ex);
		return this;
	}


	@SafeVarargs
	public static <S> TestSource<S> withItems(S... items) {
		return new TestSource<S>().items(items);
	}

	public static <S> Source<S> withFailure(Throwable ex) {
		assert (ex instanceof IOException) ||
				(ex instanceof InterruptedException) ||
				(ex instanceof RuntimeException) ||
				(ex instanceof Error);
		return new TestSource<S>().failure(ex);
	}


	@SuppressWarnings("unchecked")
	@Override
	protected @Nullable T receiveItem() throws IOException, InterruptedException {
		initIterator();
		if (this.iterator.hasNext()) {
			Object next = this.iterator.next();
			return switch (next) {
				case IOException ex -> throw ex;
				case InterruptedException ex -> throw ex;
				case RuntimeException ex -> throw ex;
				case Error ex -> throw ex;
				case Throwable ex -> throw new RuntimeException(ex); // shouldn't happen, checked Exceptions rejected
				default -> (T) next;
			};
		}
		return null;
	}

	private void initIterator() {
		if (this.iterator == null) {
			this.iterator = this.items.iterator();
		}
	}

}
