package io.spring.streamsource;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing {@link Source}s.
 */
public class TestSourceUtils {

	/**
	 * Collect the Source to a List.
	 */
	public static <T> List<T> toList(Source<T> source) throws IOException, InterruptedException {
		List<T> items = new ArrayList<>();
		while (source.receiveNext()) {
			items.add(source.next());
		}
		return items;
	}

}
