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
internal open class DefaultCassandraConverter {

    protected fun convert(offset: AtomicLong, value: List<Row>): List<CassandraRecord<Map<CqlIdentifier, Any?>>> {
        return if (value.isNotEmpty()) {
            val metaInfo = getMetaInfo(value.first())
            value.map {
                val rowAsMap = metaInfo.columnTypes.map { (name, type) ->
                    name to it.get(name, type)
                }.toMap()

                CassandraRecord(
                    source = metaInfo.keyspace,
                    offset = offset.getAndIncrement(),
                    value = rowAsMap
                )
            }
        } else {
            emptyList()
        }
    }

    internal fun getMetaInfo(row: Row) = MetaInfo(row)
}
