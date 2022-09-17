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