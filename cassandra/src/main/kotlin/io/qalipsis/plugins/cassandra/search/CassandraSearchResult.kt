package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraRecord

/**
 * Wrapper for the result of search query procedure in Cassandra.
 *
 * @author Svetlana Paliashchuk
 *
 * @property input input for the search step.
 * @property records list of Cassandra records retrieved from DB.
 * @property meters meters of the search step.
 *
 */
class CassandraSearchResult<I>(
    val input: I,
    val records: List<CassandraRecord<Map<CqlIdentifier, Any?>>>,
    val meters: CassandraQueryMeters
) : Iterable<CassandraRecord<Map<CqlIdentifier, Any?>>> {

    override fun iterator(): Iterator<CassandraRecord<Map<CqlIdentifier, Any?>>> {
        return records.iterator()
    }
}