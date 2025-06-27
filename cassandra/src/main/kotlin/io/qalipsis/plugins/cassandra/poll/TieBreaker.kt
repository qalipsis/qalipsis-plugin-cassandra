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
