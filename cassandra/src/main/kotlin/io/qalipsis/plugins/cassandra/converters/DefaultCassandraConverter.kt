package io.qalipsis.plugins.cassandra.converters

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.Row
import io.qalipsis.plugins.cassandra.CassandraRecord
import java.util.concurrent.atomic.AtomicLong

/**
 * Default converter of Cassandra database, used by specific converters.
 *
 * @author Gabriel Moraes
 */
internal open class     DefaultCassandraConverter {

    protected fun convert(offset: AtomicLong, value: List<Row>): List<CassandraRecord<Map<CqlIdentifier, Any?>>> {

        val metaInfo = getMetaInfo(value.first())
        return value.map {
            val rowAsMap = metaInfo.columnTypes.map { (name, type) ->
                name to it.get(name, type)
            }.toMap()

            CassandraRecord(
                source = metaInfo.keyspace,
                offset = offset.getAndIncrement(),
                value = rowAsMap
            )
        }
    }

    internal fun getMetaInfo(row: Row) = MetaInfo(row)
}
