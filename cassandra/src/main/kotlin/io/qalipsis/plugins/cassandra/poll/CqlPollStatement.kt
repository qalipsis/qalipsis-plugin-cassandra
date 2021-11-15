package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.cql.Row

/**
 * CQL statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Maxim Golokhov
 */
interface CqlPollStatement {

    /**
     * Prepares a pair of Query string with placeholders and Parameters.
     */
    fun compose(): Pair<String, List<Any>>

    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(rows: List<Row>)

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()
}
