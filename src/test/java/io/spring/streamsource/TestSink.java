package io.spring.streamsource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.params.shadow.de.siegmar.fastcsv.util.Nullable;

/**
 * A {@link Sink} for testing that collects all sent items and records completion.
 * @param <T> the type of items sent to the Sink
 */
public final class TestSink<T> implements Sink<T> {

	private final Map<Predicate<T>, ItemHandler<T>> handlerMap = new LinkedHashMap<>();

	private final List<T> sentItems = new ArrayList<>();

	private volatile boolean complete;

	private volatile @Nullable Throwable completionException;


	private TestSink() {
	}


	public TestSink<T> itemHandler(T targetItem, ItemHandler<T> handler) {
		this.handlerMap.put(item -> (item == targetItem), handler);
		return this;
	}

	public TestSink<T> exceptionOnItem(T targetItem, Throwable ex) {
		this.handlerMap.put(item -> (item == targetItem), i -> {
			if (ex instanceof IOException ioex) {
				throw ioex;
			}
			if (ex instanceof RuntimeException rex) {
				throw rex;
			}
			if (ex instanceof Error e) {
				throw e;
			}
			throw new RuntimeException("Simulated error on item" + i, ex);
		});
		return this;
	}

	public List<T> getSentItems() {
		return this.sentItems;
	}

	public boolean isComplete() {
		return this.complete;
	}

	public @Nullable Throwable getCompletionException() {
		return this.completionException;
	}


	// Sink methods

	@Override
	public void send(T item) throws IOException, InterruptedException {
		this.sentItems.add(item);
		for (Predicate<T> predicate : this.handlerMap.keySet()) {
			if (predicate.test(item)) {
				ItemHandler<T> handler = this.handlerMap.get(predicate);
				handler.handle(item);
				break;
			}
		}
	}

	@Override
	public void complete() {
		this.complete = true;
	}

	@Override
	public void completeExceptionally(Throwable ex) {
		this.complete = true;
		this.completionException = ex;
	}


	public static <T> TestSink<T> create() {
		return new TestSink<>();
	}


	@FunctionalInterface
	public interface ItemHandler<T> {

		void handle(T item) throws IOException, InterruptedException;

	}

}
