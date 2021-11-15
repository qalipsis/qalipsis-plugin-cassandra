package io.qalipsis.plugins.cassandra.save

/**
 * Wrapper for the result of save records procedure in Cassandra.
 *
 * @author Svetlana Paliashchuk
 *
 * @property input the data to save in Cassandra.
 * @property meters metrics of the save step.
 *
 */
data class CassandraSaveResult<I>(
    val input: I,
    val meters: CassandraSaveQueryMeters
)
