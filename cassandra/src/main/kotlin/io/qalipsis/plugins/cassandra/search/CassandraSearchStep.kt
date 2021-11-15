package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetConverter
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements.SelectStatement
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform any kind of query to get records from Cassandra.
 *
 * @property sessionBuilder supplier Cassandra session
 * @property cassandraQueryClient client to use to execute the search for the current step
 * @property converter used to return to results to the output channel
 * @property parametersFactory closure to generate a list for the query parameters
 * @property queryFactory closure to generate the string for the query
 *
 * @author Gabriel Moraes
 */
internal class CassandraSearchStep<I>(
    id: StepId,
    retryPolicy: RetryPolicy?,
    private val sessionBuilder: CqlSessionBuilder,
    private val queryFactory: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val parametersFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<Any>),
    private val converter: CassandraResultSetConverter<CassandraQueryResult, Any, I>,
    private val cassandraQueryClient: CassandraQueryClient
) : AbstractStep<I, Pair<I, List<CassandraRecord<Map<CqlIdentifier, Any?>>>>>(id, retryPolicy) {

    companion object{
        private val SESSION_TIMEOUT = Duration.ofSeconds(30)
    }

    private lateinit var session: CqlSession

    override suspend fun start(context: StepStartStopContext) {
        cassandraQueryClient.start(context)
        session = sessionBuilder.buildAsync().asSuspended().get(SESSION_TIMEOUT)
    }

    override suspend fun execute(context: StepContext<I, Pair<I, List<CassandraRecord<Map<CqlIdentifier, Any?>>>>>) {
        val input = context.receive()

        val rowIndex = AtomicLong()
        val query = queryFactory(context, input)
        val parameters = parametersFactory(context, input)

        checkQueryAndArguments(query, parameters)

        val result = cassandraQueryClient.execute(session, query, parameters, context.toEventTags())

        @Suppress("UNCHECKED_CAST")
        converter.supply(rowIndex, result, input, context as StepOutput<Any>)
    }

    private fun checkQueryAndArguments(query: String, parameters: List<Any>) {
        val parsedStatement = QueryProcessor.parseStatement(query)

        val parsedStatementAsRaw = parsedStatement as? SelectStatement.RawStatement ?: throw IllegalArgumentException("Query must be a select statement")

        require(parsedStatementAsRaw.boundVariables.size() == parameters.size) { "Parameters list should have the same size then the parameters to bind" }
    }

    override suspend fun stop(context: StepStartStopContext) {
        try {
            session.closeAsync().asSuspended().get(SESSION_TIMEOUT)
        } catch (e: Exception) {
            CompletableFutures.getUninterruptibly(session.forceCloseAsync().toCompletableFuture())
        }
        cassandraQueryClient.stop(context)
    }

}
