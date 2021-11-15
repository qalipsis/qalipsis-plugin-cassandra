package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlSession
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.cassandra.CassandraQueryResult

/**
 * Client to query from Cassandra.
 *
 * @author Gabriel Moraes
 */
interface CassandraQueryClient {

    suspend fun start(context: StepStartStopContext)

    suspend fun stop(context: StepStartStopContext)

    /**
     * Executes a query and returns the wrapper for metrics and list of results.
     */
    suspend fun execute(
        session: CqlSession,
        query: String,
        parameters: List<Any>,
        contextEventTags: Map<String, String>
    ): CassandraQueryResult
}


