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

package io.qalipsis.plugins.cassandra.save

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.sync.asSuspended
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext


/**
 * Implementation of [CassandraSaveQueryClient].
 * Client to save records in Cassandra.
 *
 * @property metrics the metrics for the query operation.
 * @property eventsLogger the logger for events to track what happens during save query execution.
 *
 * @author Svetlana Paliashchuk
 */
internal class CassandraSaveQueryClientImpl(
    private val ioCoroutineContext: CoroutineContext,
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
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
            recordsToBeSent = counter("$meterPrefix-saving-documents", tags)
            timeToSuccess = timer("$meterPrefix-time-to-response", tags)
            timeToFailure = timer("$meterPrefix-time-to-failure", tags)
            savedDocuments = counter("$meterPrefix-saved-documents", tags)
            failedDocuments = counter("$meterPrefix-failed-documents", tags)
        }
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(recordsToBeSent!!)
            remove(timeToSuccess!!)
            remove(timeToFailure!!)
            remove(savedDocuments!!)
            remove(failedDocuments!!)
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

        val timeToResponse = withContext(ioCoroutineContext) {
            val requestStart = System.nanoTime()
            try {
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
