package io.qalipsis.plugins.cassandra.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
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
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Gabriel Moraes
 */
@CleanMockkRecordedCalls
internal class CassandraResultSetSingleConverterTest {

    @Test
    internal fun `should deserialize and count the records one by one`() = runBlockingTest {
        //given
        val converter = spyk(CassandraResultSetSingleRecordConverter<Any>())

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

        val rows = listOf(row, rowSecond)
        val result = CassandraQueryResult(
            rows = rows,
            meters = CassandraQueryMeters()
        )

        //when
        val channel = Channel<Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>>(capacity = 2)
        val input = relaxedMockk<Any> { }
        converter.supply(
            AtomicLong(0),
            result,
            input,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })
        val results = listOf(channel.receive(), channel.receive())

        //then
        assertThat(results).all {
            hasSize(2)
            index(0).all {
                prop(Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::first).isNotNull()
                prop("offset") {
                    Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).offset
                }.isEqualTo(0)
                prop("source") {
                    Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).source
                }.isEqualTo("testkeyspace")
                prop("value") { Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).value }.all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("testvalue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("testvalue")
                }
            }
            index(1).all {
                prop("input") {
                    Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::first.call(it)
                }.isNotNull()
                prop("offset") {
                    Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).offset
                }.isEqualTo(1)
                prop("source") {
                    Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).source
                }.isEqualTo("testkeyspace")
                prop("value") { Pair<Any, CassandraRecord<Map<CqlIdentifier, Any?>>>::second.call(it).value }.all {
                    hasSize(2)
                    key(CqlIdentifier.fromInternal("name1")).isEqualTo("anotherValue")
                    key(CqlIdentifier.fromInternal("name2")).isEqualTo("anotherValue")
                }
            }
        }
    }
}
