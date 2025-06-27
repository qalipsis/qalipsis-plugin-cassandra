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

package io.qalipsis.plugins.cassandra.save

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
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
    id: StepName,
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

        val meters = cassandraSaveQueryClient.execute(session, tableName, columns, rows, context.toEventTags())
        context.send(CassandraSaveResult(input, meters))
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
