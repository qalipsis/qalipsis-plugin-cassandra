package io.qalipsis.plugins.cassandra.converters

import io.qalipsis.api.context.StepOutput
import java.util.concurrent.atomic.AtomicLong

/**
 * Converts a value read from cassandra rows into a data that can be forwarded to next steps and sends it to the output channel.
 *
 * @param R type of the object to convert
 * @param O type of the result
 * @param I type of the Input
 *
 * @author Gabriel moraes
 */
interface CassandraResultSetConverter<R, O, I>{

    /**
     * Sends [value] to the [output] channel in any form.
     *
     * @param offset an reference to the offset, it is up to the implementation to increment it
     * @param value input value to send after any conversion to the output
     * @param input received in the channel to be send along with the data after conversion
     * @param output channel to received the data after conversion
     */
    suspend fun supply(offset: AtomicLong, value: R, input: I, output: StepOutput<O>)
}
