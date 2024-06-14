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
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import io.qalipsis.plugins.cassandra.converters.CassandraBatchRecordConverter
import io.qalipsis.plugins.cassandra.converters.CassandraSingleRecordConverter
import io.qalipsis.plugins.cassandra.search.CassandraQueryClientImpl
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyNever
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration

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

        val recordsConverter: DatasourceObjectConverter<CassandraQueryResult, out Any> = relaxedMockk()
        every { spiedConverter.buildConverter(any(), refEq(spec)) } returns recordsConverter

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
                prop("sessionBuilder").isNotNull()
                prop("cqlPollStatement").isSameAs(cqlPollStatement)
                prop("pollPeriod").isEqualTo(Duration.ofSeconds(10))
                prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                    prop("eventsLogger").isSameAs(eventsLogger)
                    prop("meterRegistry").isNull()
                }
            }
            prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
            prop("converter").isNotNull().isSameAs(recordsConverter)
        }
        verifyOnce { spiedConverter.buildConverter(eq(creationContext.createdStep!!.name), refEq(spec)) }
        verifyNever { spiedConverter.buildConverter(neq(creationContext.createdStep!!.name), any()) }

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

        val recordsConverter: DatasourceObjectConverter<CassandraQueryResult, out Any> = relaxedMockk()
        every { spiedConverter.buildConverter(any(), refEq(spec)) } returns recordsConverter

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
                prop("sessionBuilder").isNotNull()
                prop("cqlPollStatement").isSameAs(cqlPollStatement)
                prop("pollPeriod").isEqualTo(Duration.ofSeconds(10))
                prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                    prop("eventsLogger").isNull()
                    prop("meterRegistry").isSameAs(meterRegistry)
                }
            }
            prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
            prop("converter").isNotNull().isSameAs(recordsConverter)
        }
        verifyOnce { spiedConverter.buildConverter(eq(creationContext.createdStep!!.name), refEq(spec)) }
        verifyNever { spiedConverter.buildConverter(neq(creationContext.createdStep!!.name), any()) }

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
    internal fun `should build batch converter`() {
        // given
        val spec = CassandraPollStepSpecificationImpl()

        // when
        val converter = converter.buildConverter("my-step", spec)

        // then
        assertThat(converter).isInstanceOf(CassandraBatchRecordConverter::class)

        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build single converter`() {
        // given
        val spec = CassandraPollStepSpecificationImpl()
        spec.flattenOutput = true

        // when
        val converter = converter.buildConverter("my-step", spec)

        // then
        assertThat(converter).isInstanceOf(CassandraSingleRecordConverter::class)


        confirmVerified(meterRegistry)
    }

}
