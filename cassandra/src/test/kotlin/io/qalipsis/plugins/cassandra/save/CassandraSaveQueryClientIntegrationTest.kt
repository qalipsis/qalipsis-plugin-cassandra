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
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
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
        val metersTags = relaxedMockk<Tags>()
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }
        val eventsLogger = relaxedMockk<EventsLogger>()
        val recordsToBeSent = relaxedMockk<Counter>()
        val timeToSuccess = relaxedMockk<Timer>()
        val timeToFailure = relaxedMockk<Timer>()
        val savedDocuments = relaxedMockk<Counter>()
        val failedDocuments = relaxedMockk<Counter>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("cassandra-save-saving-documents", refEq(metersTags)) } returns recordsToBeSent
            every { timer("cassandra-save-time-to-response", refEq(metersTags)) } returns timeToSuccess
            every { timer("cassandra-save-time-to-failure", refEq(metersTags)) } returns timeToFailure
            every { counter("cassandra-save-saved-documents", refEq(metersTags)) } returns savedDocuments
            every { counter("cassandra-save-failed-documents", refEq(metersTags)) } returns failedDocuments
        }
        val rows = listOf(CassandraSaveRow(42, "'2020-10-20T12:38:56'", "'Truck #1'", "'Leaving office geofence'"))
        val columns = listOf("dummy_node_id", "event_timestamp", "device_name", "event_name")
        val tableName = "tracker"
        val tags: Map<String, String> = emptyMap()
        val saveClient = CassandraSaveQueryClientImpl(testDispatcherProvider.io(), eventsLogger, meterRegistry)
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
            recordsToBeSent.increment(1.0)
            timeToSuccess.record(refEq(eventCaptor.captured[1] as Duration))
            savedDocuments.increment(1.0)
        }
        assertThat(eventCaptor.captured.toList()).all {
            index(0).all {
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
        val saveClient = CassandraSaveQueryClientImpl(testDispatcherProvider.io(), null, null)

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
            val saveClient = CassandraSaveQueryClientImpl(testDispatcherProvider.io(), null, null)

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
        val saveClient = CassandraSaveQueryClientImpl(testDispatcherProvider.io(), null, null)

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
