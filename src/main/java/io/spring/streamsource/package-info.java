/**
 * Core abstractions for imperative style streaming of data.
 *
 * <p>The package is null-marked: types are non-null unless annotated
 * {@link org.jspecify.annotations.Nullable}. That includes the item type of a Source,
 * which the implementation relies on, since it uses {@code null} internally to mean
 * "no item"; a {@code Source<@Nullable String>} is therefore not supported.
 */
@NullMarked
package io.spring.streamsource;

import org.jspecify.annotations.NullMarked;
