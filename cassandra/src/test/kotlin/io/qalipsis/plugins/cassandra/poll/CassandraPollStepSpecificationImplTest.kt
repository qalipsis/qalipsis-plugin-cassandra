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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isTrue
import assertk.assertions.prop
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.TestScenarioFactory
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.cassandra.cassandra
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration
import io.qalipsis.plugins.cassandra.configuration.DefaultValues
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 *
 * @author Maxim Golokhov
 */
internal class CassandraPollStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario with default values`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.cassandra().poll {
            name = "my-step"
            connect {
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.LOCAL
                datacenterName = "test_datacenter"
            }
            query("test")
            parameters(listOf(1, "two"))
            tieBreaker {
                name = "name"
                type = GenericType.STRING
            }
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(CassandraPollStepSpecificationImpl::class).all {
            prop(CassandraPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(CassandraPollStepSpecificationImpl::serversConfig).all {
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.LOCAL)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter")
            }
            prop(CassandraPollStepSpecificationImpl::query).isEqualTo("test")
            prop(CassandraPollStepSpecificationImpl::parameters).isEqualTo(listOf(1, "two"))
            prop(CassandraPollStepSpecificationImpl::tieBreakerConfig).isEqualTo(TieBreaker("name", GenericType.STRING))

            // check default values
            prop(CassandraPollStepSpecificationImpl::serversConfig).all {
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(1)
                    index(0).isEqualTo(DefaultValues.server)
                }
            }
            prop(CassandraPollStepSpecificationImpl::pollPeriod).isEqualTo(
                Duration.ofSeconds(DefaultValues.pollDurationInSeconds))
            prop(CassandraPollStepSpecificationImpl::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(CassandraPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.cassandra().poll {
            name = "my-step"
            connect {
                servers = listOf("localhost:7777", "10.42.42.42:7778")
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.LOCAL
                datacenterName = "test_datacenter"
            }
            query("test")
            parameters(listOf(1, "two"))
            tieBreaker {
                name = "name"
                type = GenericType.STRING
            }
            pollDelay(Duration.ofSeconds(1L))
            monitoring {
                meters = true
                events = true
            }
            broadcast(123, Duration.ofSeconds(20))
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(CassandraPollStepSpecificationImpl::class).all{
            prop(CassandraPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(CassandraPollStepSpecificationImpl::serversConfig).all {
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(2)
                    index(0).isEqualTo("localhost:7777")
                    index(1).isEqualTo("10.42.42.42:7778")
                }
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.LOCAL)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter"
                )
            }
            prop(CassandraPollStepSpecificationImpl::query).isEqualTo("test")
            prop(CassandraPollStepSpecificationImpl::parameters).isEqualTo(listOf(1, "two"))
            prop(CassandraPollStepSpecificationImpl::tieBreakerConfig).isEqualTo(TieBreaker("name", GenericType.STRING))

            prop(CassandraPollStepSpecificationImpl::pollPeriod).isEqualTo(Duration.ofSeconds(1L))
            prop(CassandraPollStepSpecificationImpl::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            prop(CassandraPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }
}
