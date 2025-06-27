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
internal class CqlPollStatementImpl(
    private val query: String,
    private val parameters: List<Any>,
    private val tieBreaker: TieBreaker
) : CqlPollStatement {
    private var sortingColumns: Map<String, Order> = mapOf()

    private var previousTieBreakerValue: Map<String, Any> = emptyMap()
    private var isUsingTieBreakerCondition = false

    private var parsedInitialQuery =
        QueryProcessor.parseStatement(query) as? SelectStatement.RawStatement
            ?: throw IllegalArgumentException("Query must be a select statement")

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
        if (sortingColumns.keys.isEmpty()) {
            throw IllegalArgumentException("At least one sorting field should be set in the query")
        }
        if (tieBreaker.name() !in sortingColumns.keys.first()) {
            throw IllegalArgumentException("The tie-breaker should be set as the first sorting column")
        }
    }

    private fun parseOrder(order: Boolean): Order {
        return when (order) {
            false -> Order.ASC
            true -> Order.DESC
        }
    }

    private fun getOperator(): String {
        return when (sortingColumns[tieBreaker.name()]) {
            Order.ASC -> ">="
            Order.DESC -> "<="
            else -> throw IllegalStateException("Unsupported type for ordering statement")
        }
    }

    private fun formQuery(): String {
        val cqlStatementBuilder = StringBuilder(currentCqlStatement)
        if (previousTieBreakerValue.isNotEmpty() && !isUsingTieBreakerCondition) {

            val insertPosition = currentCqlStatement.indexOf("order", 0, true)

            var tieBreakerCondition: String = if (parsedInitialQuery.whereClause.relations.isNotEmpty()) {
                "AND "
            } else {
                "WHERE "
            }
            tieBreakerCondition += "${tieBreaker.name()} ${getOperator()} ? "
            cqlStatementBuilder.insert(insertPosition, tieBreakerCondition)

            isUsingTieBreakerCondition = true
        }

        currentCqlStatement = cqlStatementBuilder.toString().lowercase()
        return currentCqlStatement
    }

    override fun saveTieBreakerValueForNextPoll(rows: List<Row>) {
        if (rows.isNotEmpty()) {
            val tieBreakerType = tieBreaker.type!!
            val tieBreakerValue = rows.lastOrNull()?.get(tieBreaker.name(), tieBreakerType)
            if (tieBreakerValue != null) {
                previousTieBreakerValue = mapOf(tieBreaker.name() to tieBreakerValue)
            }
        }
    }

    override fun compose(): Pair<String, List<Any>> {
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

