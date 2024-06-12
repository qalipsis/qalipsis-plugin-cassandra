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
