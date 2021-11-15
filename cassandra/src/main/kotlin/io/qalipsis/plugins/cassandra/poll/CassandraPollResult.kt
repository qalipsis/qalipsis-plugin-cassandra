package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraRecord

/**
 * Wrapper for the result of poll in Cassandra.
 *
 * @author Svetlana Paliashchuk
 *
 * @property records list of Cassandra records.
 * @property meters meters of the poll step.
 *
 */
class CassandraPollResult(
    val records: List<CassandraRecord<Map<CqlIdentifier, Any?>>>,
    val meters: CassandraQueryMeters
) : Iterable<CassandraRecord<Map<CqlIdentifier, Any?>>> {
    override fun iterator(): Iterator<CassandraRecord<Map<CqlIdentifier, Any?>>> {
        return records.iterator()
    }
}