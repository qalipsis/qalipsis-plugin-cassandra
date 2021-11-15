package io.qalipsis.plugins.cassandra.save

import com.datastax.oss.driver.api.core.CqlSession
import io.qalipsis.api.context.StepStartStopContext

/**
 * Client to save rows in Cassandra.
 *
 * @author Svetlana Paliashchuk
 */
internal interface CassandraSaveQueryClient {

    suspend fun start(context: StepStartStopContext)

    suspend fun stop(context: StepStartStopContext)

    /**
     * Executes a query.
     */
    suspend fun execute(
        session: CqlSession,
        tableName: String,
        columns: List<String>,
        rows: List<CassandraSaveRow>,
        contextEventTags: Map<String, String>
    ): CassandraSaveQueryMeters
}


