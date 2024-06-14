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

package io.qalipsis.plugins.cassandra.poll

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.cassandra.AbstractCassandraIntegrationTest
import io.qalipsis.plugins.cassandra.poll.catadioptre.init
import io.qalipsis.plugins.cassandra.search.CassandraQueryClientImpl
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Data class intended for easier comparison in tests.
 *
 * @author Maxim Golokhov
 */
internal data class Item(val dummyNodeId: Int, val timestamp: Long, val device: String, val event: String) {
    /**
     * Creates object from Cassandra's Row.
     */
    constructor(row: Row) :
            this(
                dummyNodeId = row.getInt("DUMMY_NODE_ID"),
                timestamp = row.getInstant("EVENT_TIMESTAMP")!!.toEpochMilli(),
                device = row.getString("DEVICE_NAME")!!,
                event = row.getString("EVENT_NAME")!!
            )

    /**
     * Creates object from positional values, intended data from CSV file
     * the first value, dummyNodeId, for all items equals 42.
     */
    constructor(positionalValues: List<String>) :
            this(
                dummyNodeId = 42,
                timestamp = stringTimeToEpochMilli(positionalValues[0]),
                device = positionalValues[1],
                event = positionalValues[2]
            )

    companion object {
        private fun stringTimeToEpochMilli(string: String): Long {
            val formatter = DateTimeFormatter.ofPattern("yyyy-M-d'T'HH:mm:ss")
            val time = LocalDateTime.parse(string, formatter)
            return time.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli()
        }
    }
}

internal class CassandraIterativeReaderIntegrationTest : AbstractCassandraIntegrationTest() {

    private lateinit var reader: CassandraIterativeReader

    private val context = relaxedMockk<StepStartStopContext>()

    @BeforeEach
    @Timeout(20)
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
    @Timeout(10)
    internal fun `should consume all the data in the table and monitor`() = testDispatcherProvider.run {
        //given
        executeScript("input/batch0.cql")
        val testQuery = "SELECT * FROM keySpaceTest.TRACKER WHERE DUMMY_NODE_ID = 42 ORDER BY EVENT_TIMESTAMP ASC"
        val expected = session.execute(testQuery).all().map { Item(it) }
        val pollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM TRACKER WHERE DUMMY_NODE_ID = ? ORDER BY EVENT_TIMESTAMP ASC",
            parameters = listOf(42),
            tieBreaker = TieBreaker("EVENT_TIMESTAMP", GenericType.INSTANT),
        )

        reader = CassandraIterativeReader(
            ioCoroutineScope = this,
            sessionBuilder = sessionBuilder,
            cqlPollStatement = pollStatement,
            pollPeriod = Duration.ofSeconds(1),
            cassandraQueryClient = CassandraQueryClientImpl(null, null, "poll")
        )

        //when
        reader.start(relaxedMockk())

        //then
        val actual = reader.next().rows.map { Item(it) }
        assertThat(actual).isEqualTo(expected)

        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(20)
    internal fun `should poll 3 batches with ascending order`() = testDispatcherProvider.run {
        //given
        val pollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM TRACKER WHERE DUMMY_NODE_ID = ? ORDER BY EVENT_TIMESTAMP ASC",
            parameters = listOf(42),
            tieBreaker = TieBreaker("EVENT_TIMESTAMP", GenericType.INSTANT),
        )

        reader = CassandraIterativeReader(
            ioCoroutineScope = this,
            sessionBuilder = sessionBuilder,
            cqlPollStatement = pollStatement,
            pollPeriod = Duration.ofSeconds(1),
            cassandraQueryClient = CassandraQueryClientImpl(null, null, "poll")
        )

        val session = sessionBuilder.build()

        // when
        reader.init()

        executeScript("input/batch0.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        executeScript("input/batch1.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        executeScript("input/batch2.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        //then
        val expectedBatch0 = readFromCsv("expected/asc/batch0.csv")
        var actual = reader.next().rows.map { Item(it) }
        assertThat(actual).isEqualTo(expectedBatch0)

        val expectedBatch1 = readFromCsv("expected/asc/batch1.csv")
        actual = reader.next().rows.map { Item(it) }
        assertThat(actual).isEqualTo(expectedBatch1)

        val expectedBatch2 = readFromCsv("expected/asc/batch2.csv")
        actual = reader.next().rows.map { Item(it) }
        assertThat(actual).isEqualTo(expectedBatch2)

        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(20)
    internal fun `should poll 3 batches with descending order`() = testDispatcherProvider.run {
        //given
        val pollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM TRACKER WHERE DUMMY_NODE_ID = ? ORDER BY EVENT_TIMESTAMP DESC",
            parameters = listOf(42),
            tieBreaker = TieBreaker("EVENT_TIMESTAMP", GenericType.INSTANT),
        )

        reader = CassandraIterativeReader(
            ioCoroutineScope = this,
            sessionBuilder = sessionBuilder,
            cqlPollStatement = pollStatement,
            pollPeriod = Duration.ofSeconds(1),
            cassandraQueryClient = CassandraQueryClientImpl(null, null, "poll")
        )

        val session = sessionBuilder.build()

        //when
        reader.init()

        executeScript("input/batch2.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        executeScript("input/batch1.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        executeScript("input/batch0.cql")
        reader.coInvokeInvisible<Unit>("poll", session, context)

        //then
        val expectedBatch2 = readFromCsv("expected/desc/batch2.csv")
        val actual0 = reader.next().rows.map { Item(it) }
        assertThat(actual0).isEqualTo(expectedBatch2)

        val expectedBatch1 = readFromCsv("expected/desc/batch1.csv")
        val actual1 = reader.next().rows.map { Item(it) }
        assertThat(actual1).isEqualTo(expectedBatch1)

        val expectedBatch0 = readFromCsv("expected/desc/batch0.csv")
        val actual2 = reader.next().rows.map { Item(it) }
        assertThat(actual2).isEqualTo(expectedBatch0)

        reader.stop(relaxedMockk())
    }
}
