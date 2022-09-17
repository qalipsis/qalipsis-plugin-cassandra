/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
