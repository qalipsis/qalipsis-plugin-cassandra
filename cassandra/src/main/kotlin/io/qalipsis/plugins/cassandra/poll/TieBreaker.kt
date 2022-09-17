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

import com.datastax.oss.driver.api.core.type.reflect.GenericType
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull

/**
 * The boundary value to select the next batch of data.
 * @property name is used in cql query.
 * @property type is a hint for datastax driver how to interpret a value.
 *
 * @author Maxim Golokhov
 */
data class TieBreaker (
    @field:NotBlank var name: String = "",
    @field:NotNull var type: GenericType<*>? = null
){
    fun name(): String{
        return this.name.lowercase()
    }
}
