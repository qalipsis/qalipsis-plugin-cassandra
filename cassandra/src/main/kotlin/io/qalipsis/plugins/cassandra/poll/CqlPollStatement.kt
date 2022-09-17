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

package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.cql.Row

/**
 * CQL statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Maxim Golokhov
 */
interface CqlPollStatement {

    /**
     * Prepares a pair of Query string with placeholders and Parameters.
     */
    fun compose(): Pair<String, List<Any>>

    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(rows: List<Row>)

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()
}
