package io.qalipsis.plugins.cassandra.save

/**
 * Wrapper for the passed records to save in Cassandra.
 *
 * @author Svetlana Paliashchuk
 */
class CassandraSaveRow(vararg args: Any?) {
    val args = args.toList()
}

