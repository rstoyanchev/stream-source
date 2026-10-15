/**
 * A POC of MCP's Streamable HTTP transport (2026-07-28 protocol) over
 * {@code io.spring.streamsource}: {@link mcp.McpServer} answers a request with a
 * {@code Source<String>}, either a single response or a stream of notifications followed
 * by the response, and {@link mcp.McpClient} consumes it. See the README.
 *
 * <p>The package is null-marked, like {@code io.spring.streamsource} itself: types are
 * non-null unless annotated {@link org.jspecify.annotations.Nullable}.
 */
@NullMarked
package mcp;

import org.jspecify.annotations.NullMarked;
