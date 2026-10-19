/**
 * A bridge between {@link io.spring.streamsource.Source} and a JDK
 * {@link java.util.concurrent.Flow.Publisher}, through {@link FlowSourceAdapter}, with no
 * extra dependency.
 *
 * <p>{@code reactor.adapter.streamsource.SourceAdapter} offers the same bridge
 * for Reactor.
 *
 * <p>The package is null-marked: types are non-null unless annotated
 * {@link org.jspecify.annotations.Nullable}. That includes the item type of a Source,
 * which the implementation relies on, since it uses {@code null} internally to mean
 * "no item"; a {@code Source<@Nullable String>} is therefore not supported.
 */
@NullMarked
package io.spring.streamsource.flow;

import org.jspecify.annotations.NullMarked;
