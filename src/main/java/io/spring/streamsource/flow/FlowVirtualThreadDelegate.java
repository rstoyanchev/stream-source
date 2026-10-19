package io.spring.streamsource.flow;

import java.util.concurrent.ExecutorService;

/**
 * Internal delegate for virtual thread handling on JDK 21.
 * This is a dummy version for reachability on JDK <21.
 *
 * @see FlowSourceAdapter
 */
final class FlowVirtualThreadDelegate {

	public FlowVirtualThreadDelegate() {
		throw new UnsupportedOperationException("Virtual threads not supported on JDK <21");
	}

	public ExecutorService newVirtualThreadPerTaskExecutor() {
		throw new UnsupportedOperationException();
	}

}
