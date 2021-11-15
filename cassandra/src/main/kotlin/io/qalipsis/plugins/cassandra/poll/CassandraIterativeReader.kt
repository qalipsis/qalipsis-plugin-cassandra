package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.search.CassandraQueryClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration


/**
 * Implementation of [DatasourceIterativeReader] to poll rows from Apache Cassandra.
 *
 * @property sessionBuilder builder for the DB connection
 * @property cqlPollStatement statement to execute
 * @property pollPeriod duration between the end of a poll and the start of the next one
 * @property running running state of the reader
 * @property pollingJob instance of the background job polling data from the database
 *
 * @author Maxim Golokhov
 */
internal class CassandraIterativeReader(
    private val ioCoroutineScope: CoroutineScope,
    private val sessionBuilder: CqlSessionBuilder,
    private val cqlPollStatement: CqlPollStatement,
    private val pollPeriod: Duration,
    private val cassandraQueryClient: CassandraQueryClient,
    private var resultChannelFactory: () -> Channel<CassandraQueryResult> = { Channel(Channel.UNLIMITED) }
) : DatasourceIterativeReader<CassandraQueryResult> {

    private lateinit var session: CqlSession

    private var pollingJob: Job? = null

    private var running = false

    private var resultChannel: Channel<CassandraQueryResult>? = null

    override fun start(context: StepStartStopContext) {
        init()
        running = true
        pollingJob = ioCoroutineScope.launch {
            try {
                cassandraQueryClient.start(context)
                while (running) {
                    poll(session, context)
                    delay(pollPeriod.toMillis())
                }
            } finally {
                resultChannel?.close()
                resultChannel = null
            }
        }
    }

    @KTestable
    private fun init() {
        cqlPollStatement.reset()
        resultChannel = resultChannelFactory()
        session = sessionBuilder.build()
    }

    private suspend fun poll(session: CqlSession, context: StepStartStopContext) {
        try {
            val (queryWithPlaceholders, parameters) = cqlPollStatement.compose()
            val result = cassandraQueryClient.execute(session, queryWithPlaceholders, parameters, context.toEventTags())
            resultChannel!!.send(result)
            cqlPollStatement.saveTieBreakerValueForNextPoll(result.rows)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Logs the error but allows next poll.
            log.error(e) { e.message }
        }
    }

    override fun stop(context: StepStartStopContext) {
        running = false
        runCatching {
            session.close()
        }
        runCatching {
            runBlocking {
                pollingJob?.cancelAndJoin()
                cassandraQueryClient.stop(context)
            }
        }
        pollingJob = null
        cqlPollStatement.reset()
    }

    override suspend fun hasNext(): Boolean = running

    override suspend fun next(): CassandraQueryResult = resultChannel!!.receive()

    companion object {
        @JvmStatic
        private val log = logger()
    }
}


