package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.cql.Row
import io.aerisconsulting.catadioptre.KTestable
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.cql3.statements.SelectStatement

/**
 * Implementation of [CqlPollStatement], to be used when polling from a Cassandra database.
 *
 * @property query CQL statement to be executed when polling.
 * @property parameters list for query parameters.
 * @property tieBreaker object with field name and type to be used as query tie breaker when polling.
 * @property sortingColumns used to validate query ordering using a tie breaker.
 * @property previousTieBreakerValue value for previous tie breaker.
 * @property isUsingTieBreakerCondition variable to control the need to add tie breaker clause to Cql statement.
 * @property parsedInitialQuery makes use of cassandra all (Query Parser)[https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/cql3/QueryProcessor.java] to parse a query string.
 * @property currentCqlStatement save current query state.
 *
 * @author Maxim Golokhov
 * @author Gabriel Moraes
 */
internal class CqlPollStatementImpl (
    private val query: String,
    private val parameters: List<Any>,
    private val tieBreaker: TieBreaker
): CqlPollStatement {
    private var sortingColumns: Map<String, Order> = mapOf()

    private var previousTieBreakerValue: Map<String, Any> = emptyMap()
    private var isUsingTieBreakerCondition = false

    private var parsedInitialQuery =
        QueryProcessor.parseStatement(query) as? SelectStatement.RawStatement ?:
            throw IllegalArgumentException("Query must be a select statement")

    private var currentCqlStatement: String = query

    init {
        sortingColumns = parseOrderQuery()
        tieBreakerShouldBeFirstSortingColumn()
    }

    @KTestable
    private fun parseOrderQuery() =
        parsedInitialQuery.parameters.orderings.entries.associate {
            Pair(
                it.key.toCQLString().lowercase(),
                parseOrder(it.value)
            )
        }

    private fun tieBreakerShouldBeFirstSortingColumn() {
        if (tieBreaker.name() !in sortingColumns.keys.first())
            throw IllegalArgumentException("The tie-breaker should be set as the first sorting column")
    }

    private fun parseOrder(order: Boolean): Order {
        return when (order) {
            false -> Order.ASC
            true -> Order.DESC
        }
    }

    private fun getOperator(): String{
        return when(sortingColumns[tieBreaker.name()]) {
            Order.ASC -> ">="
            Order.DESC -> "<="
            else -> throw IllegalStateException("Unsupported type for ordering statement")
        }
    }

    private fun formQuery(): String {
        val cqlStatementBuilder = StringBuilder(currentCqlStatement)
        if (previousTieBreakerValue.isNotEmpty() && !isUsingTieBreakerCondition){

            val insertPosition = currentCqlStatement.indexOf("order", 0, true)

            var tieBreakerCondition: String = if (parsedInitialQuery.whereClause.relations.isNotEmpty()){
                "AND "
            }else{
                "WHERE "
            }
            tieBreakerCondition += "${tieBreaker.name()} ${getOperator()} ? "
            cqlStatementBuilder.insert(insertPosition, tieBreakerCondition)

            isUsingTieBreakerCondition = true
        }

        currentCqlStatement = cqlStatementBuilder.toString().lowercase()
        return currentCqlStatement
    }

    private fun getPreviousTieBreaker(rows: List<Row>): Any {
        return rows.last().get(tieBreaker.name(), tieBreaker.type!!)!!
    }

    override fun saveTieBreakerValueForNextPoll(rows: List<Row>) {
        previousTieBreakerValue = mapOf(tieBreaker.name() to getPreviousTieBreaker(rows))
    }

    override fun compose(): Pair <String, List<Any>> {
        val queryWithPlaceholders = formQuery()
        return Pair(queryWithPlaceholders, (parameters + previousTieBreakerValue.values))
    }

    override fun reset() {
        previousTieBreakerValue = emptyMap()
        isUsingTieBreakerCondition = false
    }

    internal enum class Order {
        ASC, DESC
    }
}

