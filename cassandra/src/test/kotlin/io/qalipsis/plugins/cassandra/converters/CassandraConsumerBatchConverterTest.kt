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
import io.qalipsis.plugins.cassandra.poll.CassandraPollResult
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
internal class CassandraConsumerBatchConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should deserialize and count the records`() = testDispatcherProvider.runTest {
        //given
        val converter = spyk(CassandraBatchRecordConverter())

        every { converter.getMetaInfo(any()) } returns MetaInfo(
            "testkeyspace",
            mapOf(
                CqlIdentifier.fromInternal("name1") to GenericType.STRING,
                CqlIdentifier.fromInternal("name2") to GenericType.INSTANT
            )
        )

        val row = mockk<Row> {
            every { get(any<CqlIdentifier>(), any<GenericType<*>>()) } returns "testvalue"
        }

        val rows = listOf(
            row,
            row,
        )
        val result = CassandraQueryResult(
            rows = rows,
            meters = CassandraQueryMeters()
        )

        //when
        val channel = Channel<CassandraPollResult>(capacity = 1)
        converter.supply(
            AtomicLong(0),
            result,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })
        val results = channel.receive()

        //then
        assertThat(results.records).all {
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
