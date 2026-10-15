/**
 * A bridge between {@link io.spring.streamsource.Source} and a Reactor
 * {@link reactor.core.publisher.Flux}, through {@link SourceAdapter}, with Reactor
 * context propagation support.
 *
 * <p>This package is provided by the {@code reactor-adapter} module, which brings
 * {@code io.projectreactor:reactor-core} as a dependency, so that the core
 * {@code stream-source} module does not need it.
 *
 * <p>The package is null-marked: types are non-null unless annotated
 * {@link org.jspecify.annotations.Nullable}.
 */
@NullMarked
package reactor.adapter.streamsource;

import org.jspecify.annotations.NullMarked;
