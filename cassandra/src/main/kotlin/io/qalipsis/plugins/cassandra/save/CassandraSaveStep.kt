package io.qalipsis.plugins.cassandra.save

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.api.sync.asSuspended
import java.time.Duration

/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform save records in Cassandra.
 *
 * @property cassandraSaveQueryClient client to execute save query for the current step.
 * @property sessionBuilder supplier Cassandra session.
 * @property tableName name of the table.
 * @property columns list of column names.
 * @property rowsFactory closure to generate a list for the rows to save.
 *
 * @author Svetlana Paliashchuk
 */
internal class CassandraSaveStep<I>(
    id: StepId,
    retryPolicy: RetryPolicy?,
    private val cassandraSaveQueryClient: CassandraSaveQueryClient,
    private val sessionBuilder: CqlSessionBuilder,
    private val tableName: suspend (ctx: StepContext<*, *>, input: I) -> String,
    private val columns: suspend (ctx: StepContext<*, *>, input: I) -> List<String>,
    private val rowsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<CassandraSaveRow>
) : AbstractStep<I, CassandraSaveResult<I>>(id, retryPolicy) {

    companion object {
        private val SESSION_TIMEOUT = Duration.ofSeconds(30)
    }

    private lateinit var session: CqlSession

    override suspend fun start(context: StepStartStopContext) {
        cassandraSaveQueryClient.start(context)
        session = sessionBuilder.buildAsync().asSuspended().get(SESSION_TIMEOUT)
    }

    override suspend fun execute(context: StepContext<I, CassandraSaveResult<I>>) {
        val input = context.receive()
        val rows = rowsFactory(context, input)
        val tableName = tableName(context, input)
        val columns = columns(context, input)

        val metrics = cassandraSaveQueryClient.execute(session, tableName, columns, rows, context.toEventTags())
        context.send(CassandraSaveResult(input, meters = metrics))
    }

    override suspend fun stop(context: StepStartStopContext) {
        try {
            session.closeAsync().asSuspended().get(SESSION_TIMEOUT)
        } catch (e: Exception) {
            CompletableFutures.getUninterruptibly(session.forceCloseAsync().toCompletableFuture())
        }
        cassandraSaveQueryClient.stop(context)
    }

}
