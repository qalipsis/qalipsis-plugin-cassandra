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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isSameAs
import assertk.assertions.prop
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.Timer
import io.qalipsis.plugins.cassandra.AbstractCassandraIntegrationTest
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Instant

/**
 *
 * @author Gabriel Moraes
 */
internal class CassandraQueryClientIntegrationTest : AbstractCassandraIntegrationTest() {

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
    fun `should succeed when sending query with multiple results and monitor`() = testDispatcherProvider.run {
        // given
        val tags: Map<String, String> = emptyMap()
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toEventTags() } returns tags
            every { scenarioName } returns "scenario-test"
            every { stepName } returns "step-test"
        }
        val eventsLogger = relaxedMockk<EventsLogger>()
        val recordsCount = relaxedMockk<Counter>()
        val timeToSuccess = relaxedMockk<Timer>()
        val timeToFailure = relaxedMockk<Timer>()
        val successCounter = relaxedMockk<Counter>()
        val failureCounter = relaxedMockk<Counter>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("scenario-test", "step-test", "cassandra-test-fetched-records", refEq(tags)) } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every { timer("scenario-test", "step-test", "cassandra-test-time-to-response", refEq(tags)) } returns timeToSuccess
            every { timer("scenario-test", "step-test", "cassandra-test-time-to-failure", refEq(tags)) } returns timeToFailure
            every { counter("scenario-test", "step-test", "cassandra-test-success", refEq(tags)) } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { counter("scenario-test", "step-test", "cassandra-test-failure", refEq(tags)) } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
        }

        executeScript("input/batch0.cql")
        val paramsBuilder = listOf(42, Instant.parse("2020-10-20T12:34:21Z"))

        val queryBuilder =
            """SELECT * FROM TRACKER WHERE 
               DUMMY_NODE_ID = ? AND EVENT_TIMESTAMP = ?""".trimIndent()

        val searchClient = CassandraQueryClientImpl(testDispatcherProvider.io(), eventsLogger, meterRegistry, "test")
        searchClient.start(startStopContext)

        // when
        val results = searchClient.execute(session, queryBuilder, paramsBuilder, tags).rows

        // then
        searchClient.stop(relaxedMockk())
        assertThat(results).all {
            hasSize(3)
            index(0).all {
                prop("DEVICE_NAME") { it.getString("DEVICE_NAME") }.isEqualTo("Car #1")
                prop("EVENT_NAME") { it.getString("EVENT_NAME") }.isEqualTo("Ignition on")
            }
            index(1).all {
                prop("DEVICE_NAME") { it.getString("DEVICE_NAME") }.isEqualTo("Car #2")
                prop("EVENT_NAME") { it.getString("EVENT_NAME") }.isEqualTo("Ignition on")
            }
            index(2).all {
                prop("DEVICE_NAME") { it.getString("DEVICE_NAME") }.isEqualTo("Truck #1")
                prop("EVENT_NAME") { it.getString("EVENT_NAME") }.isEqualTo("Ignition on")
            }

        }

        val eventCaptor = slot<Array<*>>()
        verify {
            eventsLogger.info("cassandra.test.success", capture(eventCaptor), timestamp = any(), tags = refEq(tags))
        }
        verify {
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            successCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            failureCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            recordsCount.increment(3.0)
            timeToSuccess.record(refEq(eventCaptor.captured[1] as Duration))
            successCounter.increment()
        }
        assertThat(eventCaptor.captured.toList()).all {
            index(0).isEqualTo(3)
            index(1).isNotNull().isInstanceOf(Duration::class.java).isGreaterThan(Duration.ZERO)
        }

        confirmVerified(
            recordsCount,
            timeToSuccess,
            timeToFailure,
            successCounter,
            failureCounter,
            eventsLogger
        )
    }

    @Test
    @Timeout(20)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        // given
        executeScript("input/batch1.cql")
        val paramsBuilder = listOf(42, Instant.parse("2020-10-20T12:38:56Z"), "Truck #1")
        val queryBuilder =
            """SELECT * FROM TRACKER WHERE 
               DUMMY_NODE_ID = ? AND EVENT_TIMESTAMP = ? AND DEVICE_NAME = ?""".trimIndent()
        val tags: Map<String, String> = emptyMap()
        val searchClient = CassandraQueryClientImpl(testDispatcherProvider.io(), null, null, "search")

        // when
        val results = searchClient.execute(session, queryBuilder, paramsBuilder, tags).rows

        //then
        searchClient.stop(relaxedMockk())
        assertThat(results).all {
            hasSize(1)
            index(0).all {
                prop("DEVICE_NAME") { it.getString("DEVICE_NAME") }.isEqualTo("Truck #1")
                prop("EVENT_NAME") { it.getString("EVENT_NAME") }.isEqualTo("Leaving office geofence")
            }
        }
    }

    @Test
    @Timeout(20)
    fun `should fail when sending invalid query`() = testDispatcherProvider.run {
        // given
        val tags: Map<String, String> = emptyMap()
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toEventTags() } returns tags
            every { scenarioName } returns "scenario-test"
            every { stepName } returns "step-test"
        }
        val eventsLogger = relaxedMockk<EventsLogger>()
        val recordsCount = relaxedMockk<Counter>()
        val timeToSuccess = relaxedMockk<Timer>()
        val timeToFailure = relaxedMockk<Timer>()
        val successCounter = relaxedMockk<Counter>()
        val failureCounter = relaxedMockk<Counter>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("scenario-test", "step-test", "cassandra-test-fetched-records", refEq(tags)) } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every { timer("scenario-test", "step-test", "cassandra-test-time-to-response", refEq(tags)) } returns timeToSuccess
            every { timer("scenario-test", "step-test", "cassandra-test-time-to-failure", refEq(tags)) } returns timeToFailure
            every { counter("scenario-test", "step-test", "cassandra-test-success", refEq(tags)) } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { counter("scenario-test", "step-test", "cassandra-test-failure", refEq(tags)) } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
        }

        val paramsBuilder = listOf(42)
        val queryBuilder =
            """SELECT * FROM TRACKER WHERE 
               DUMMY_NODE_ID = ? AND EVENT_TIMESTAMP = ? AND DEVICE_NAME = ?""".trimIndent()

        val searchClient = CassandraQueryClientImpl(testDispatcherProvider.io(), eventsLogger, meterRegistry, "test")
        searchClient.start(startStopContext)

        // when
        val exception = assertThrows<InvalidQueryException> {
            searchClient.execute(sessionBuilder.build(), queryBuilder, paramsBuilder, tags)
        }

        // then
        val eventCaptor = slot<Array<*>>()
        verify {
            eventsLogger.warn("cassandra.test.failure", capture(eventCaptor), timestamp = any(), tags = refEq(tags))
        }
        verify {
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            successCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            failureCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            timeToFailure.record(refEq(eventCaptor.captured[1] as Duration))
            failureCounter.increment()
        }
        assertThat(eventCaptor.captured.toList()).all {
            index(0).isSameAs(exception)
            index(1).isNotNull().isInstanceOf(Duration::class.java).isGreaterThan(Duration.ZERO)
        }

        searchClient.stop(relaxedMockk())

        confirmVerified(
            recordsCount,
            timeToSuccess,
            timeToFailure,
            successCounter,
            failureCounter,
            eventsLogger
        )
    }

}
