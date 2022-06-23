package io.qalipsis.plugins.cassandra.search

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
import java.math.BigDecimal


/**
 *
 * @author Gabriel Moraes
 */
internal class CassandraSearchStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    fun `should add minimal configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.cassandra().search {
            name = "my-search-step"
            connect {
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.LOCAL
                datacenterName = "test_datacenter"
            }
            query { _, _ ->
                "SELECT * FROM TRACKER"
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(CassandraSearchStepSpecificationImpl::class).all {
            prop("name") { CassandraSearchStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-search-step")
            prop(CassandraSearchStepSpecificationImpl<*>::serversConfig).all {
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.LOCAL)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter")
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(1)
                    index(0).isEqualTo(DefaultValues.server)
                }
            }
            prop(CassandraSearchStepSpecificationImpl<*>::queryFactory).isNotNull()
            prop(CassandraSearchStepSpecificationImpl<*>::monitoringConfig).isNotNull()
            prop(CassandraSearchStepSpecificationImpl<*>::parametersFactory).isNotNull()
            prop(CassandraSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::meters).isFalse()
                prop(StepMonitoringConfiguration::events).isFalse()
            }
            prop(CassandraSearchStepSpecificationImpl<*>::flattenOutput).isFalse()
        }

        val queryFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>(
                "queryFactory")
        assertThat(queryFactory(relaxedMockk(), relaxedMockk())).isEqualTo("""SELECT * FROM TRACKER""")

        val paramsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<Any>>(
                "parametersFactory")
        assertThat(paramsFactory(relaxedMockk(), relaxedMockk())).hasSize(0)
    }


    @Test
    fun `should add a complete configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.cassandra().search {
            name = "my-search-step"
            connect {
                keyspace = "test_keyspace"
                datacenterProfile = DriverProfile.PEER
                datacenterName = "test_datacenter"
                servers = listOf("localhost:27017", "localhost:27018")
            }
            query { _, _ ->
                "SELECT * FROM TRACKER"
            }

            parameters { _, _ ->
                listOf("1", BigDecimal.TEN, 123)
            }

            monitoring {
                meters = true
                events = true
            }

        }.flatten()

        assertThat(previousStep.nextSteps[0]).isInstanceOf(CassandraSearchStepSpecificationImpl::class).all {
            prop("name") { CassandraSearchStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-search-step")
            prop(CassandraSearchStepSpecificationImpl<*>::serversConfig).all {
                prop(CassandraServerConfiguration::keyspace).isEqualTo("test_keyspace")
                prop(CassandraServerConfiguration::datacenterProfile).isEqualTo(DriverProfile.PEER)
                prop(CassandraServerConfiguration::datacenterName).isEqualTo("test_datacenter")
                prop(CassandraServerConfiguration::servers).all {
                    hasSize(2)
                    index(0).isEqualTo("localhost:27017")
                    index(1).isEqualTo("localhost:27018")
                }
            }
            prop(CassandraSearchStepSpecificationImpl<*>::queryFactory).isNotNull()
            prop(CassandraSearchStepSpecificationImpl<*>::parametersFactory).isNotNull()
            prop(CassandraSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::meters).isTrue()
                prop(StepMonitoringConfiguration::events).isTrue()
            }
            prop(CassandraSearchStepSpecificationImpl<*>::flattenOutput).isTrue()
        }

        val queryFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>(
                "queryFactory")
        assertThat(queryFactory(relaxedMockk(), relaxedMockk())).isEqualTo("""SELECT * FROM TRACKER""")

        val paramsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<Any>>(
                "parametersFactory")
        assertThat(paramsFactory(relaxedMockk(), relaxedMockk())).hasSize(3)
    }
}
