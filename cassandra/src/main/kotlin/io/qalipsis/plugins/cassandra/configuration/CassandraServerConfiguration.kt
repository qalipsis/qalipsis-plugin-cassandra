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

package io.qalipsis.plugins.cassandra.configuration

import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty


/**
 * Connection for operations using Datastax driver.
 * @property servers host:port, default [DefaultValues.server].
 * @property keyspace the top-level database object that controls the replication for the object it contains at each
 * datacenter in the cluster.
 * @property datacenterProfile an execution profile to encapsulate a group of settings that can then be associated with
 * individual queries.
 * @property datacenterName datacenter name.
 */
data class CassandraServerConfiguration internal constructor(
    @field:NotEmpty var servers: List<String> = listOf(DefaultValues.server),
    @field:NotBlank var keyspace: String = "",
    var datacenterProfile: DriverProfile = DriverProfile.DEFAULT,
    @field:NotBlank var datacenterName: String = "",
)

enum class DriverProfile {
    LOCAL, PEER, DEFAULT
}

internal object DefaultValues {
    const val pollDurationInSeconds = 10L
    const val server = "localhost:9042"
}
