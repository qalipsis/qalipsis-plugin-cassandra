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

package io.qalipsis.plugins.cassandra.save

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.cassandra.cassandra
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration
import io.qalipsis.plugins.cassandra.configuration.DefaultValues
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension


/**
 *
 * @author Svetlana Paliashchuk
 */
internal class CassandraSaveStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    fun `should add minimal configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.cassandra().save {
            name = "my-save-step"
            connect {
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.LOCAL
                datacenterName = "test_datacenter"
            }
            table { _, _ ->
                "test"
            }
            columns { _, _ ->
                listOf("column")
            }
            rows { _, _ ->
                listOf(CassandraSaveRow())
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(CassandraSaveStepSpecificationImpl::class).all {
            prop("name") { CassandraSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(CassandraSaveStepSpecificationImpl<*>::serversConfig).all {
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.LOCAL)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter")
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(1)
                    index(0).isEqualTo(DefaultValues.server)
                }
            }
            prop(CassandraSaveStepSpecificationImpl<*>::tableName).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::columnsConfig).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::rowsFactory).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }

        val rowsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<CassandraSaveRow>>(
                "rowsFactory"
            )
        assertThat(rowsFactory(relaxedMockk(), relaxedMockk())).hasSize(1)
        val columnsConfig =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<String>>(
                "columnsConfig"
            )
        assertThat(columnsConfig(relaxedMockk(), relaxedMockk())).hasSize(1)
    }


    @Test
    fun `should add a complete configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.cassandra().save {
            name = "my-save-step"
            connect {
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.LOCAL
                datacenterName = "test_datacenter"
            }
            table { _, _ ->
                "test"
            }
            columns { _, _ ->
                listOf("col1", "col2", "col3", "col4")
            }
            rows { _, _ ->
                listOf(CassandraSaveRow("my-value1", true, null, 12.45),
                    CassandraSaveRow("my-value2", false, null, 100))
            }
            monitoring{
                events = true
                meters = true
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(CassandraSaveStepSpecificationImpl::class).all {
            prop("name") { CassandraSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(CassandraSaveStepSpecificationImpl<*>::serversConfig).all {
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.LOCAL)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter")
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(1)
                    index(0).isEqualTo(DefaultValues.server)
                }
            }
            prop(CassandraSaveStepSpecificationImpl<*>::tableName).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::columnsConfig).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::rowsFactory).isNotNull()
            prop(CassandraSaveStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
        }

        val rowsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<CassandraSaveRow>>(
                "rowsFactory"
            )

        assertThat(rowsFactory(relaxedMockk(), relaxedMockk())).hasSize(2)
        val columnsConfig =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<String>>(
                "columnsConfig")
        assertThat(columnsConfig(relaxedMockk(), relaxedMockk())).hasSize(4)
    }
}
