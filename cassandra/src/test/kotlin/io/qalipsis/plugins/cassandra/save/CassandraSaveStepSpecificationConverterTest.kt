package io.qalipsis.plugins.cassandra.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 *
 * @author Svetlana Paliashchuk
 */
@Suppress("UNCHECKED_CAST")
@WithMockk
internal class CassandraSaveStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<CassandraSaveStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var ioCoroutineContext: CoroutineContext

    private val rowsFactory: (suspend (ctx: StepContext<*, *>, input: Any) -> List<CassandraSaveRow>) = relaxedMockk()

    private val columns: (suspend (ctx: StepContext<*, *>, input: Any) -> List<String>) = relaxedMockk()

    private val tableName: (suspend (ctx: StepContext<*, *>, input: Any) -> String) = relaxedMockk()

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<CassandraSaveStepSpecificationImpl<*>>()))
            .isTrue()

    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert with retry policy and events only`() = runBlockingTest {
        // given
        val spec = CassandraSaveStepSpecificationImpl<Any>()
        spec.let {
            it.name = "my-step"
            it.serversConfig = CassandraServerConfiguration(
                servers = listOf("localhost:7777"),
                keyspace = "test_keyspace",
                datacenterProfile = DriverProfile.LOCAL,
                datacenterName = "test_datacenter"
            )
            it.tableName = tableName
            it.columnsConfig = columns
            it.rowsFactory = rowsFactory
            it.retryPolicy = mockedRetryPolicy

            it.monitoring {
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(CassandraSaveStep::class).all {
            prop("id").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNotNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("tableName").isEqualTo(tableName)
            prop("columns").isEqualTo(columns)
            prop("rowsFactory").isEqualTo(rowsFactory)
            prop("cassandraSaveQueryClient").isNotNull().isInstanceOf(CassandraSaveQueryClientImpl::class).all {
                prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                prop("eventsLogger").isSameAs(eventsLogger)
                prop("meterRegistry").isNull()
            }
        }
    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert without retry policy but with meters`() = runBlockingTest {
        // given
        val spec = CassandraSaveStepSpecificationImpl<Any>()
        spec.let {
            it.name = "my-step"
            it.serversConfig = CassandraServerConfiguration(
                servers = listOf("localhost:7777"),
                keyspace = "test_keyspace",
                datacenterProfile = DriverProfile.LOCAL,
                datacenterName = "test_datacenter"
            )
            it.tableName = tableName
            it.columnsConfig = columns
            it.rowsFactory = rowsFactory
            it.monitoring {
                meters = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(CassandraSaveStep::class).all {
            prop("id").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("tableName").isEqualTo(tableName)
            prop("columns").isEqualTo(columns)
            prop("rowsFactory").isEqualTo(rowsFactory)
            prop("cassandraSaveQueryClient").isNotNull().isInstanceOf(CassandraSaveQueryClientImpl::class).all {
                prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                prop("eventsLogger").isNull()
                prop("meterRegistry").isSameAs(meterRegistry)
            }
        }
    }
}
