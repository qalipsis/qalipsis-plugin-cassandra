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

package io.qalipsis.plugins.cassandra

import com.datastax.oss.driver.api.core.cql.Row
import io.qalipsis.plugins.cassandra.save.CassandraSaveResult

/**
 * A wrapper for meters and rows used to initialize [CassandraSaveResult].
 *
 * @property meters metrics of the search step.
 * @property rows result of search query procedure in Cassandra.
 *
 * @author Svetlana Paliashchuk
 */
class CassandraQueryResult(
    val rows: List<Row>,
    val meters: CassandraQueryMeters
)