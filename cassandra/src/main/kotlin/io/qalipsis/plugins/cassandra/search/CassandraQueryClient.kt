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

import com.datastax.oss.driver.api.core.CqlSession
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.cassandra.CassandraQueryResult

/**
 * Client to query from Cassandra.
 *
 * @author Gabriel Moraes
 */
interface CassandraQueryClient {

    suspend fun start(context: StepStartStopContext)

    suspend fun stop(context: StepStartStopContext)

    /**
     * Executes a query and returns the wrapper for metrics and list of results.
     */
    suspend fun execute(
        session: CqlSession,
        query: String,
        parameters: List<Any>,
        contextEventTags: Map<String, String>
    ): CassandraQueryResult
}


