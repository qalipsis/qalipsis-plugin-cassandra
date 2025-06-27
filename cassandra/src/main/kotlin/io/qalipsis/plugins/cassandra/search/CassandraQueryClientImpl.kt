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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.Row
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import java.time.Duration

/**
 * Implementation of [CassandraQueryClient].
 * Client to query from Cassandra.
 *
 * @param stepType the qualifier of the step to name the events and metrics
 *
 * @author Gabriel Moraes
 */
internal class CassandraQueryClientImpl(
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?,
    stepType: String,
) : CassandraQueryClient {

    private val eventPrefix = "cassandra.${stepType}"

    private val meterPrefix = "cassandra-${stepType}"

    private var recordsCount: Counter? = null

    private var timeToSuccess: Timer? = null

    private var timeToFailure: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            recordsCount = counter(scenarioName, stepName, "$meterPrefix-fetched-records", tags).report {
                display(
                    format = "attempted req: %,.0f bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToSuccess = timer(scenarioName, stepName, "$meterPrefix-time-to-response", tags)
            timeToFailure = timer(scenarioName, stepName, "$meterPrefix-time-to-failure", tags)
            successCounter = counter(scenarioName, stepName, "$meterPrefix-success", tags).report {
                display(
                    format = "\u2713 %,.0f successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            failureCounter = counter(scenarioName, stepName, "$meterPrefix-failure", tags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 1,
                    Counter::count
                )
            }
        }
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            recordsCount = null
            timeToSuccess = null
            timeToFailure = null
            successCounter = null
            failureCounter = null
        }
    }

    /**
     * Executes a query and returns the value object that contains meters and list of results.
     */
    override suspend fun execute(
        session: CqlSession,
        query: String,
        parameters: List<Any>,
        contextEventTags: Map<String, String>
    ): CassandraQueryResult {

        val preparedQuery = session.prepare(query)
        val boundStatement = preparedQuery.bind(*(parameters).toTypedArray())
        val requestStart = System.nanoTime()

        return try {
            val results = mutableListOf<Row>()
            fetch(session.executeAsync(boundStatement).asSuspended().get(), results)
            val timeToResponse = Duration.ofNanos(System.nanoTime() - requestStart)

            eventsLogger?.info(
                "${eventPrefix}.success",
                arrayOf(results.size, timeToResponse),
                tags = contextEventTags
            )

            timeToSuccess?.record(timeToResponse)
            recordsCount?.increment(results.size.toDouble())
            successCounter?.increment()

            CassandraQueryResult(
                rows = results,
                meters = CassandraQueryMeters(results.size, timeToResponse)
            )
        } catch (e: Exception) {
            val timeToResponse = Duration.ofNanos(System.nanoTime() - requestStart)
            eventsLogger?.warn("${eventPrefix}.failure", arrayOf(e, timeToResponse), tags = contextEventTags)
            failureCounter?.increment()
            timeToFailure?.record(timeToResponse)

            throw e
        }
    }

    private suspend fun fetch(asyncResultSet: AsyncResultSet, results: MutableList<Row>) {
        results.addAll(asyncResultSet.currentPage().toList())
        if (asyncResultSet.hasMorePages()) {
            fetch(asyncResultSet.fetchNextPage().asSuspended().get(), results)
        }
    }
}


