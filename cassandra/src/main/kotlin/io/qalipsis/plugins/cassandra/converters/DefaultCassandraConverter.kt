/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
