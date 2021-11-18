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
import java.util.stream.Collectors
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
        val query = buildBatchQuery(tableName, columns, rows, failedDocumentsCount)
        eventsLogger?.debug("$eventPrefix.saving-documents", rows.size, tags = contextEventTags)
        recordsToBeSent?.increment(rows.size.toDouble())

        val requestStart = System.nanoTime()
        return try {
            val timeToResponse = withContext(ioCoroutineContext) {
                session.executeAsync(query).asSuspended().get()
                Duration.ofNanos(System.nanoTime() - requestStart)
            }
            val savedDocumentsCount = rows.size - failedDocumentsCount.get()
            require(savedDocumentsCount > 0) { "None of the rows could be saved" }

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

            CassandraSaveQueryMeters(
                rows.size, timeToResponse, savedDocumentsCount, failedDocumentsCount.get()
            )
        } catch (e: Exception) {
            val timeToResponse = Duration.ofNanos(System.nanoTime() - requestStart)
            eventsLogger?.warn("$eventPrefix.failure", arrayOf(timeToResponse, e), tags = contextEventTags)
            timeToFailure?.record(timeToResponse)

            throw e
        }
    }

    private fun buildBatchQuery(
        tableName: String,
        columns: List<String>,
        rows: List<CassandraSaveRow>,
        failedDocumentsCount: AtomicInteger
    ): String {
        var query = """BEGIN BATCH """
        val quotedTableName = "\"$tableName\""
        val quotedColumns: String = columns.stream()
            .map { s -> "\"" + s + "\"" }
            .collect(Collectors.joining(", "))
        rows.forEach {
            if (checkColumnsAndArgumentsSizes(columns, it)) {
                query =
                    query.plus("""INSERT INTO $quotedTableName ($quotedColumns) VALUES (${it.args.joinToString()});""")
            } else failedDocumentsCount.incrementAndGet()
        }
        query = query.plus(""" APPLY BATCH;""")
        return query
    }

    private fun checkColumnsAndArgumentsSizes(columns: List<String>, row: CassandraSaveRow): Boolean {
        return columns.size == row.args.size
    }

}
