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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.prop
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.Row
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.cassandra.AbstractCassandraIntegrationTest
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * @author Svetlana Paliashchuk
 */
internal class CassandraSaveQueryClientIntegrationTest : AbstractCassandraIntegrationTest() {

    @BeforeEach
    @Timeout(10)
    internal fun setUpEach() {
        session = sessionBuilder.build()
    }

    @AfterEach
    @Timeout(10)
    internal fun tearDownEach() {
        session.execute("TRUNCATE TRACKER")
        super.tearDown()
    }

    @Test
    @Timeout(20)
    fun `should succeed when save a single row and monitor`() = testDispatcherProvider.run {
        // given
        val tags: Map<String, String> = emptyMap()
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns tags
            every { scenarioName } returns "scenario-test"
            every { stepName } returns "step-test"
        }
        val eventsLogger = relaxedMockk<EventsLogger>()
        val recordsToBeSent = relaxedMockk<Counter>()
        val timeToSuccess = relaxedMockk<Timer>()
        val timeToFailure = relaxedMockk<Timer>()
        val savedDocuments = relaxedMockk<Counter>()
        val failedDocuments = relaxedMockk<Counter>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every {
                counter(
                    "scenario-test",
                    "step-test",
                    "cassandra-save-saving-documents",
                    refEq(tags)
                )
            } returns recordsToBeSent
            every { recordsToBeSent.report(any()) } returns recordsToBeSent
            every {
                timer(
                    "scenario-test",
                    "step-test",
                    "cassandra-save-time-to-response",
                    refEq(tags)
                )
            } returns timeToSuccess
            every {
                timer(
                    "scenario-test",
                    "step-test",
                    "cassandra-save-time-to-failure",
                    refEq(tags)
                )
            } returns timeToFailure
            every {
                counter(
                    "scenario-test",
                    "step-test",
                    "cassandra-save-saved-documents",
                    refEq(tags)
                )
            } returns savedDocuments
            every { savedDocuments.report(any()) } returns savedDocuments
            every {
                counter(
                    "scenario-test",
                    "step-test",
                    "cassandra-save-failed-documents",
                    refEq(tags)
                )
            } returns failedDocuments
            every { failedDocuments.report(any()) } returns failedDocuments
        }
        val rows = listOf(CassandraSaveRow(42, "'2020-10-20T12:38:56'", "'Truck #1'", "'Leaving office geofence'"))
        val columns = listOf("dummy_node_id", "event_timestamp", "device_name", "event_name")
        val tableName = "tracker"
        val saveClient = CassandraSaveQueryClientImpl(eventsLogger, meterRegistry)
        saveClient.start(startStopContext)

        // when
        saveClient.execute(session, tableName, columns, rows, tags)
        val results = mutableListOf<Row>()
        val statement = "SELECT * FROM $tableName"
        fetch(session.executeAsync(statement).asSuspended().get(), results)

        // then
        saveClient.stop(relaxedMockk())
        assertThat(results).all {
            hasSize(1)
            index(0).all {
                prop("device_name") { it.getString("device_name") }.isEqualTo("Truck #1")
                prop("event_name") { it.getString("event_name") }.isEqualTo("Leaving office geofence")
            }
        }

        val eventCaptor = slot<Array<*>>()
        verifyOrder {
            eventsLogger.debug("cassandra.save.saving-documents", 1, timestamp = any(), tags = refEq(tags))
            eventsLogger.info(
                "cassandra.save.saved-documents",
                capture(eventCaptor),
                timestamp = any(),
                tags = refEq(tags)
            )
        }
        verify {
            recordsToBeSent.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            savedDocuments.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            failedDocuments.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            recordsToBeSent.increment(1.0)
            timeToSuccess.record(refEq(eventCaptor.captured[1] as Duration))
            savedDocuments.increment(1.0)
        }
        assertThat(eventCaptor.captured.toList()).all {
            index(0).isNotNull().isInstanceOf<AtomicInteger>().all {
                prop(AtomicInteger::get).isEqualTo(1)
            }
            index(1).isNotNull().isInstanceOf(Duration::class.java).isGreaterThan(Duration.ZERO)
        }

        confirmVerified(
            recordsToBeSent,
            timeToSuccess,
            timeToFailure,
            savedDocuments,
            failedDocuments,
            eventsLogger
        )
    }

    @Test
    @Timeout(20)
    fun `should succeed when save multiple rows`() = testDispatcherProvider.run {
        // given
        val rows = listOf(
            CassandraSaveRow(42, "'2020-10-20T12:38:56'", "'Truck #1'", "'Leaving office geofence'"),
            CassandraSaveRow(42, "'2020-10-20T12:40:14'", "'Car #1'", "'Driving over 30 kmh'"),
            CassandraSaveRow(42, "'2020-10-20T12:40:14'", "'Car #2'", "'Driving over 30 kmh'")
        )
        val columns = listOf("dummy_node_id", "event_timestamp", "device_name", "event_name")
        val tableName = "tracker"
        val tags: Map<String, String> = emptyMap()
        val saveClient = CassandraSaveQueryClientImpl(null, null)

        // when
        saveClient.execute(session, tableName, columns, rows, tags)
        val results = mutableListOf<Row>()
        val statement = "SELECT * FROM $tableName"
        fetch(session.executeAsync(statement).asSuspended().get(), results)

        // then
        saveClient.stop(relaxedMockk())
        assertThat(results).all {
            hasSize(3)
            index(0).all {
                prop("device_name") { it.getString("device_name") }.isEqualTo("Truck #1")
                prop("event_name") { it.getString("event_name") }.isEqualTo("Leaving office geofence")
            }
            index(1).all {
                prop("device_name") { it.getString("device_name") }.isEqualTo("Car #1")
                prop("event_name") { it.getString("event_name") }.isEqualTo("Driving over 30 kmh")
            }
            index(2).all {
                prop("device_name") { it.getString("device_name") }.isEqualTo("Car #2")
                prop("event_name") { it.getString("event_name") }.isEqualTo("Driving over 30 kmh")
            }
        }
    }

    @Test
    @Timeout(20)
    fun `should succeed when number of arguments in one of the rows does not match the number of columns`() =
        testDispatcherProvider.run {
            // given
            val rows = listOf(
                CassandraSaveRow(42, "'2020-10-20T12:38:56'", "'Truck #1'", "'Leaving office geofence'"),
                CassandraSaveRow(42, "'2020-10-20T12:40:14'", "'Driving over 30 kmh'"),
                CassandraSaveRow(42, "'2020-10-20T12:40:14'", "'Car #2'", "'Driving over 30 kmh'")
            )
            val columns = listOf("dummy_node_id", "event_timestamp", "device_name", "event_name")
            val tableName = "tracker"
            val tags: Map<String, String> = emptyMap()
            val saveClient = CassandraSaveQueryClientImpl(null, null)

            // when
            saveClient.execute(session, tableName, columns, rows, tags)
            val results = mutableListOf<Row>()
            val statement = "SELECT * FROM $tableName"
            fetch(session.executeAsync(statement).asSuspended().get(), results)

            // then
            saveClient.stop(relaxedMockk())
            assertThat(results).all {
                hasSize(2)
                index(0).all {
                    prop("device_name") { it.getString("device_name") }.isEqualTo("Truck #1")
                    prop("event_name") { it.getString("event_name") }.isEqualTo("Leaving office geofence")
                }
                index(1).all {
                    prop("device_name") { it.getString("device_name") }.isEqualTo("Car #2")
                    prop("event_name") { it.getString("event_name") }.isEqualTo("Driving over 30 kmh")
                }
            }
        }

    @Test
    @Timeout(20)
    fun `should fail when empty list of rows is passed`() = testDispatcherProvider.run {
        // given
        val rows: List<CassandraSaveRow> = emptyList()
        val columns = listOf("dummy_node_id", "event_timestamp", "device_name", "event_name")

        val tableName = "tracker"
        val tags: Map<String, String> = emptyMap()
        val saveClient = CassandraSaveQueryClientImpl(null, null)

        // when
        assertThrows<IllegalArgumentException> {
            saveClient.execute(session, tableName, columns, rows, tags)
        }
        val results = mutableListOf<Row>()
        val statement = "SELECT * FROM $tableName"
        fetch(session.executeAsync(statement).asSuspended().get(), results)

        // then
        saveClient.stop(relaxedMockk())
        assertThat(results).hasSize(0)
    }

    private suspend fun fetch(asyncResultSet: AsyncResultSet, results: MutableList<Row>) {
        results.addAll(asyncResultSet.currentPage().toList())
        if (asyncResultSet.hasMorePages()) {
            fetch(asyncResultSet.fetchNextPage().asSuspended().get(), results)
        }
    }
}
