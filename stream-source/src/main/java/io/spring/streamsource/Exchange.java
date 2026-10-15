package io.spring.streamsource;

/**
 * A {@link Source} and {@link Sink} combined forming a pipe of items that can be
 * both sent to and received from.
 *
 * @param <T> the type of items passed through the Exchange
 */
public interface Exchange<T> extends Source<T>, Sink<T> {

}
