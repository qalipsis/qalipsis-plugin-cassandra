package io.qalipsis.plugins.cassandra.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import io.qalipsis.plugins.cassandra.converters.CassandraBatchRecordConverter
import io.qalipsis.plugins.cassandra.search.CassandraQueryClientImpl
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import kotlin.coroutines.CoroutineContext

/**
 *
 * @author Maxim Golokhov
 */
@WithMockk
internal class CassandraPollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<CassandraPollStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var ioCoroutineScope: CoroutineScope

    @RelaxedMockK
    private lateinit var ioCoroutineContext: CoroutineContext

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<CassandraPollStepSpecificationImpl>())).isTrue()
    }

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk())).isFalse()
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(1)
    fun `should convert with event logger only`() = testDispatcherProvider.runTest {
        // given
        val spec = CassandraPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect {
                servers = listOf("localhost:7777")
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
            pollDelay(10_000L)
            broadcast(123, Duration.ofSeconds(20))
            monitoring {
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter)

        val cqlPollStatement: CqlPollStatement = relaxedMockk()
        every { spiedConverter.buildCqlStatement(refEq(spec)) } returns cqlPollStatement

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraPollStepSpecificationImpl>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(IterativeDatasourceStep::class).all {
            prop("name").isEqualTo("my-step")
            prop("reader").isNotNull().isInstanceOf(CassandraIterativeReader::class).all {
                prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                prop("sessionBuilder").isNotNull()
                prop("cqlPollStatement").isSameAs(cqlPollStatement)
                prop("pollPeriod").isEqualTo(Duration.ofSeconds(10))
                prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                    prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                    prop("eventsLogger").isSameAs(eventsLogger)
                    prop("meterRegistry").isNull()
                }
            }
            prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
            prop("converter").isNotNull().isInstanceOf(CassandraBatchRecordConverter::class)
        }
        val channelFactory = creationContext.createdStep!!
            .getProperty<CassandraIterativeReader>("reader")
            .getProperty<() -> Channel<List<Row>>>("resultChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(1)
    fun `should convert with meter registry only`() = testDispatcherProvider.runTest {
        // given
        val spec = CassandraPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect {
                servers = listOf("localhost:7777")
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
            pollDelay(10_000L)
            broadcast(123, Duration.ofSeconds(20))
            monitoring {
                meters = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter)

        val cqlPollStatement: CqlPollStatement = relaxedMockk()
        every { spiedConverter.buildCqlStatement(refEq(spec)) } returns cqlPollStatement

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraPollStepSpecificationImpl>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(IterativeDatasourceStep::class).all {
            prop("name").isEqualTo("my-step")
            prop("reader").isNotNull().isInstanceOf(CassandraIterativeReader::class).all {
                prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                prop("sessionBuilder").isNotNull()
                prop("cqlPollStatement").isSameAs(cqlPollStatement)
                prop("pollPeriod").isEqualTo(Duration.ofSeconds(10))
                prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                    prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                    prop("eventsLogger").isNull()
                    prop("meterRegistry").isSameAs(meterRegistry)
                }
            }
            prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
            prop("converter").isNotNull().isInstanceOf(CassandraBatchRecordConverter::class)
        }

        val channelFactory = creationContext.createdStep!!
            .getProperty<CassandraIterativeReader>("reader")
            .getProperty<() -> Channel<List<Row>>>("resultChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

}
