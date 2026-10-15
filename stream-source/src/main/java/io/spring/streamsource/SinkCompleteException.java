package io.spring.streamsource;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when a {@link Sink} can no longer accept items either because it has
 * been completed, or for any other reason such as a close from the
 * {@code Source} side of an {@link Exchange}.
 */
public class SinkCompleteException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = -4503267191314132852L;


	public SinkCompleteException(@Nullable String message) {
		super(message);
	}

	public SinkCompleteException(@Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
