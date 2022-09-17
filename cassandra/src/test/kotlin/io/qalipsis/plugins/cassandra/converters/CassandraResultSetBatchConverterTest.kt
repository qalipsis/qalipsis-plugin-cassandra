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
import io.qalipsis.plugins.cassandra.search.CassandraSearchResult
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.atomic.AtomicLong

@CleanMockkRecordedCalls
internal class CassandraResultSetBatchConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    fun `should convert and send output in batches`() = testDispatcherProvider.runTest {
        //given
        val converter = spyk(CassandraResultSetBatchRecordConverter<Any>())

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

        val rowSecond = mockk<Row> {
            every { get(any<CqlIdentifier>(), any<GenericType<*>>()) } returns "anotherValue"
        }

        val rows = listOf(
            row,
            rowSecond,
        )
        val result = CassandraQueryResult(
            rows = rows,
            meters = CassandraQueryMeters()
        )

        //when
        val channel = Channel<CassandraSearchResult<Any>>(capacity = 1)
        val input = relaxedMockk<Any> { }
        converter.supply(
            AtomicLong(0),
            result,
            input,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })
        val results = channel.receive()

        //then
        assertThat(results.records).all {
            hasSize(2)
            index(0).all {
                prop(
                    CassandraRecord<Map<CqlIdentifier, Any?>>::offset
                ).isEqualTo(0)
                prop(
                    CassandraRecord<Map<CqlIdentifier, Any?>>::source
                ).isEqualTo("testkeyspace")
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::value).all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("testvalue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("testvalue")
                }
            }
            index(1).all {
                prop(
                    CassandraRecord<Map<CqlIdentifier, Any?>>::offset
                ).isEqualTo(1)
                prop(
                    CassandraRecord<Map<CqlIdentifier, Any?>>::source
                ).isEqualTo("testkeyspace")
                prop(CassandraRecord<Map<CqlIdentifier, Any?>>::value).all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("anotherValue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("anotherValue")
                }
            }
        }
    }
}


