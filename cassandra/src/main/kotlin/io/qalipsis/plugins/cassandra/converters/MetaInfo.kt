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
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.qalipsis.plugins.cassandra.poll.getJavaTypeFromCqlType

/**
 * Metadata extracted from Cassandra Row in a more convenient way for [DefaultCassandraConverter]
 *
 * @author Maxim Golokhov
 */
internal data class MetaInfo(
    val keyspace:String,
    val columnTypes: Map<CqlIdentifier, GenericType<*>>
) {
    constructor(row: Row): this(
        keyspace= row.columnDefinitions.first().keyspace.toString(),
        columnTypes = row.columnDefinitions.map {
            it.name to getJavaTypeFromCqlType(it.type)
        }.toMap()
    )
}