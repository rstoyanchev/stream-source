package reactor.adapter.streamsource;

import java.util.concurrent.ExecutorService;

/**
 * Internal delegate for virtual thread handling on JDK 21.
 * This is a dummy version for reachability on JDK <21.
 *
 * @see SourceAdapter
 */
final class ReactorVirtualThreadDelegate {

	public ReactorVirtualThreadDelegate() {
		throw new UnsupportedOperationException("Virtual threads not supported on JDK <21");
	}

	public ExecutorService newVirtualThreadPerTaskExecutor() {
		throw new UnsupportedOperationException();
	}

}
