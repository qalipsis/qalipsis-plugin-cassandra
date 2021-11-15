package io.qalipsis.plugins.cassandra.poll

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.setProperty
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.plugins.cassandra.poll.CqlPollStatementImpl.Order
import io.qalipsis.plugins.cassandra.poll.catadioptre.parseOrderQuery
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
/**
 *
 * @author Maxim Golokhov
 */
internal class CqlPollStatementImplTest {
    @Test
    fun `should parse ordering clause as Map(columnName to Order)`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
                query = "SELECT * FROM test_table WHERE first_int = ? order by column0 ASC,column1 ASC , column2 DESC",
                parameters = listOf(1, "second"),
                tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )

        //when
        val actual = cqlPollStatement.parseOrderQuery()

        //then
        assertThat(actual).isEqualTo(mapOf("column0" to Order.ASC, "column1" to Order.ASC, "column2" to Order.DESC))
    }

    @Test
    internal fun `should save last value as tie breaker when ascending order`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "select * from dummy order by column_int ASC, column_string DESC",
            parameters = listOf(1, "dummy"),
            tieBreaker = TieBreaker("column_int", GenericType.INTEGER),
        )

        val rows = listOf<Row>(
            relaxedMockk {
                every { get("column_int", GenericType.INTEGER) } returns 1
                every { get("column_string", GenericType.STRING) } returns "first"
            },
            relaxedMockk {
                every { get("column_int", GenericType.INTEGER) } returns 2
                every { get("column_string", GenericType.STRING) } returns "second"
            }
        )

        //when
        cqlPollStatement.saveTieBreakerValueForNextPoll(rows)

        //then
        val actual = cqlPollStatement.getProperty<Map<String, Any>>("previousTieBreakerValue")
        assertThat(actual).isEqualTo(mapOf("column_int" to 2))
    }

    @Test
    internal fun `should save last value as tie breaker when descending order`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM test_table WHERE first_int = ? order by column_string ASC,column1 ASC , column2 DESC",
            parameters = listOf(1, "dummy"),
            tieBreaker = TieBreaker("column_string", GenericType.STRING),
        )

        cqlPollStatement.setProperty("sortingColumns", mapOf("column_int" to Order.ASC, "column_string" to Order.DESC))

        val rows = listOf<Row>(
            mockk {
                every { get("column_int", GenericType.INTEGER) } returns 1
                every { get("column_string", GenericType.STRING) } returns "first"
            },
            mockk {
                every { get("column_int", GenericType.INTEGER) } returns 2
                every { get("column_string", GenericType.STRING) } returns "second"
            }
        )

        //when
        cqlPollStatement.saveTieBreakerValueForNextPoll(rows)

        //then
        val actual = cqlPollStatement.getProperty<Map<String, Any>>("previousTieBreakerValue")
        assertThat(actual).isEqualTo(mapOf("column_string" to "second"))
    }

    @Test
    fun `should parse sorting clause and prepare query with placeholders`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM test_table WHERE first_int = ? order by column0 ASC,column1 ASC , column2 DESC",
            parameters = listOf(1),
            tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )

        //when
        val (query, params) = cqlPollStatement.compose()

        //then
        assertThat(query).isEqualTo("SELECT * FROM test_table WHERE first_int = ? order by column0 ASC,column1 ASC , column2 DESC".lowercase())
        assertThat(params).isEqualTo(listOf(1))
    }

    @Test
    fun `should parse sorting clause and prepare query with placeholders with tie breaker`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "select * from test_table where first_int = ? AND second_string = ? order by column0 ASC,column1 ASC , column2 DESC",
            parameters = listOf(1),
            tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )

        cqlPollStatement.setProperty("previousTieBreakerValue", mapOf("column0" to 10))

        //when
        val (query, params) = cqlPollStatement.compose()

        //then
        assertThat(query).isEqualTo("SELECT * FROM test_table WHERE first_int = ? AND second_string = ? AND column0 >= ? order by column0 ASC,column1 ASC , column2 DESC".lowercase())
        assertThat(params).isEqualTo(listOf(1, 10))
    }

    @Test
    fun `should parse sorting clause and prepare query with tie breaker`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "select * from test_table order by column0 ASC,column1 ASC , column2 DESC",
            parameters = emptyList(),
            tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )

        cqlPollStatement.setProperty("previousTieBreakerValue", mapOf("column0" to 10))

        //when
        val (query, params) = cqlPollStatement.compose()

        //then
        assertThat(query).isEqualTo("SELECT * FROM test_table WHERE column0 >= ? order by column0 ASC,column1 ASC , column2 DESC".lowercase())
        assertThat(params).isEqualTo(listOf(10))
    }

    @Test
    fun `should parse sorting clause and not update query with tie breaker when it is already added`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "SELECT * FROM test_table WHERE column0 >= ? order by column0 ASC,column1 ASC , column2 DESC",
            parameters = emptyList(),
            tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )

        cqlPollStatement.setProperty("previousTieBreakerValue", mapOf("column0" to 15))
        cqlPollStatement.setProperty("isUsingTieBreakerCondition", true)

        //when
        val (query, params) = cqlPollStatement.compose()

        //then
        assertThat(query).isEqualTo("SELECT * FROM test_table WHERE column0 >= ? order by column0 ASC,column1 ASC , column2 DESC".lowercase())
        assertThat(params).isEqualTo(listOf(15))
    }

    @Test
    internal fun `should fail when the tie-breaker is not in the ordering statement`() {
        assertThrows<IllegalArgumentException> {
            CqlPollStatementImpl(
                query = "SELECT * FROM test_table order by column0 ASC,column1 ASC , column2 DESC",
                parameters = listOf(1),
                tieBreaker = TieBreaker("column1", GenericType.INTEGER),
            )
        }
    }

    @Test
    internal fun `should reset saved tie-breaker by calling reset()`() {
        //given
        val cqlPollStatement = CqlPollStatementImpl(
            query = "select * from test_table WHERE first_int = ? order by column0 ASC,column1 ASC , column2 DESC",
            parameters = listOf(1),
            tieBreaker = TieBreaker("column0", GenericType.INTEGER),
        )
        cqlPollStatement.setProperty("previousTieBreakerValue", mapOf("column0" to 10))

        //when
        cqlPollStatement.reset()

        //then
        val actual = cqlPollStatement.getProperty<Map<String, Any>>("previousTieBreakerValue")
        assertTrue(actual.isEmpty())
    }
}