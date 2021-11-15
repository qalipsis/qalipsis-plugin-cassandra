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
