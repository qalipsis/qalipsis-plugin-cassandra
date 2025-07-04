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
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.sync.asSuspended
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger


/**
 * Implementation of [CassandraSaveQueryClient].
 * Client to save records in Cassandra.
 *
 * @property meterRegistry the metrics for the query operation.
 * @property eventsLogger the logger for events to track what happens during save query execution.
 *
 * @author Svetlana Paliashchuk
 */
internal class CassandraSaveQueryClientImpl(
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?
) : CassandraSaveQueryClient {

    private val eventPrefix = "cassandra.save"

    private val meterPrefix = "cassandra-save"

    private var recordsToBeSent: Counter? = null

    private var timeToSuccess: Timer? = null

    private var timeToFailure: Timer? = null

    private var savedDocuments: Counter? = null

    private var failedDocuments: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            recordsToBeSent = counter(scenarioName, stepName, "$meterPrefix-saving-documents", tags).report {
                display(
                    format = "attempted save %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToSuccess = timer(scenarioName, stepName, "$meterPrefix-time-to-response", tags)
            timeToFailure = timer(scenarioName, stepName, "$meterPrefix-time-to-failure", tags)
            savedDocuments = counter(scenarioName, stepName, "$meterPrefix-saved-documents", tags).report {
                display(
                    format = "\u2716 %,.0f successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
            failedDocuments = counter(scenarioName, stepName, "$meterPrefix-failed-documents", tags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
        }
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            recordsToBeSent = null
            timeToSuccess = null
            timeToFailure = null
            savedDocuments = null
            failedDocuments = null
        }
    }

    /**
     * Executes save query.
     */
    override suspend fun execute(
        session: CqlSession,
        tableName: String,
        columns: List<String>,
        rows: List<CassandraSaveRow>,
        contextEventTags: Map<String, String>
    ): CassandraSaveQueryMeters {
        val failedDocumentsCount = AtomicInteger()
        val savedDocumentsCount = AtomicInteger()
        eventsLogger?.debug("$eventPrefix.saving-documents", rows.size, tags = contextEventTags)
        recordsToBeSent?.increment(rows.size.toDouble())
        val queryList = mutableListOf<String>()
        rows.forEach {
            if (checkColumnsAndArgumentsSizes(columns, it)) {
                queryList.add("INSERT INTO $tableName (${columns.joinToString()}) VALUES (${it.args.joinToString()})")
            } else failedDocumentsCount.incrementAndGet()
        }

        val requestStart = System.nanoTime()
        val timeToResponse = try {
            val futures = queryList.map { session.executeAsync(it).asSuspended() }
            futures.forEach {
                if (it.get().wasApplied()) {
                    savedDocumentsCount.incrementAndGet()
                } else {
                    failedDocumentsCount.incrementAndGet()
                }
            }
            Duration.ofNanos(System.nanoTime() - requestStart)
        } catch (e: Exception) {
            val timeToResponse = Duration.ofNanos(System.nanoTime() - requestStart)
            eventsLogger?.warn("$eventPrefix.failure", arrayOf(timeToResponse, e), tags = contextEventTags)
            timeToFailure?.record(timeToResponse)
            throw e
        }
        require(savedDocumentsCount.get() > 0) { "None of the rows could be saved" }

        eventsLogger?.info(
            "$eventPrefix.saved-documents",
            arrayOf(savedDocumentsCount, timeToResponse),
            tags = contextEventTags
        )
        savedDocuments?.increment(savedDocumentsCount.toDouble())
        if (failedDocumentsCount.get() > 0) {
            eventsLogger?.warn("$eventPrefix.failed-documents", failedDocumentsCount.get(), tags = contextEventTags)
            failedDocuments?.increment(failedDocumentsCount.toDouble())
        }

        timeToSuccess?.record(timeToResponse)

        return CassandraSaveQueryMeters(
            rows.size, timeToResponse, savedDocumentsCount.get(), failedDocumentsCount.get()
        )
    }

    private fun checkColumnsAndArgumentsSizes(columns: List<String>, row: CassandraSaveRow): Boolean {
        return columns.size == row.args.size
    }
}
