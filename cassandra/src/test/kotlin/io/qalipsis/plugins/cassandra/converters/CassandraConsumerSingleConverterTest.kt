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

package io.qalipsis.plugins.cassandra.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import assertk.assertions.prop
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Maxim Golokhov
 */
@CleanMockkRecordedCalls
internal class CassandraConsumerSingleConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should deserialize and count the records`() = testDispatcherProvider.runTest {
        //given
        val converter = spyk(CassandraSingleRecordConverter())

        every { converter.getMetaInfo(any()) } returns MetaInfo(
            "testkeyspace",
            mapOf(
                CqlIdentifier.fromInternal("name1") to GenericType.STRING,
                CqlIdentifier.fromInternal("name2") to GenericType.INSTANT,
            )
        )

        val row = mockk<Row> {
            every { get(any<CqlIdentifier>(), any<GenericType<*>>()) } returns "testvalue"
        }

        val rows = listOf(row, row)

        val result = CassandraQueryResult(
            rows = rows,
            meters = CassandraQueryMeters()
        )
        //when
        val channel = Channel<CassandraRecord<Map<CqlIdentifier, Any?>>>(capacity = 2)
        converter.supply(
            AtomicLong(0),
            result,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })
        val results = listOf(channel.receive(), channel.receive())

        //then
        assertThat(results).all {
            hasSize(2)
            index(0).all {
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::offset).isEqualTo(0)
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::source).isEqualTo("testkeyspace")
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::value).all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("testvalue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("testvalue")
                }
            }
            index(1).all {
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::offset).isEqualTo(1)
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::source).isEqualTo("testkeyspace")
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::value).all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("testvalue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("testvalue")
                }
            }
        }
    }
}
