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
