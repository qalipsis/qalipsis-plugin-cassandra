/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetConverter
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
    id: StepName,
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

        val result = cassandraQueryClient.execute(session, query, parameters, context.toEventTags())

        @Suppress("UNCHECKED_CAST")
        converter.supply(rowIndex, result, input, context as StepOutput<Any>)
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
