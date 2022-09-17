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

package io.qalipsis.plugins.cassandra.search

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
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetBatchRecordConverter
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.coroutines.CoroutineContext

/**
 *
 * @author Gabriel Moraes
 */
@Suppress("UNCHECKED_CAST")
@WithMockk
internal class CassandraSearchStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<CassandraSearchStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var ioCoroutineContext: CoroutineContext

    private val queryFactory: (suspend (ctx: StepContext<*, *>, input: Any?) -> String) = relaxedMockk()

    private val paramsFactory: (suspend (ctx: StepContext<*, *>, input: Any) -> List<Any>) = relaxedMockk()

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<CassandraSearchStepSpecificationImpl<*>>()))
            .isTrue()

    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert with retry policy`() = testDispatcherProvider.runTest {
        // given
        val spec = CassandraSearchStepSpecificationImpl<Any>()
        spec.also {
            it.name = "my-step"
            it.scenario = scenarioSpecification
            it.serversConfig = CassandraServerConfiguration(
                servers = listOf("localhost:7777"),
                keyspace = "test_keyspace",
                datacenterProfile = DriverProfile.LOCAL,
                datacenterName = "test_datacenter"
            )
            it.parametersFactory = paramsFactory
            it.queryFactory = queryFactory
            it.retryPolicy = mockedRetryPolicy
            it.monitoring {
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraSearchStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(CassandraSearchStep::class).all {
            prop("name").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNotNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("queryFactory").isEqualTo(queryFactory)
            prop("parametersFactory").isEqualTo(paramsFactory)
            prop("converter").isNotNull().isInstanceOf(CassandraResultSetBatchRecordConverter::class)
            prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                prop("eventsLogger").isSameAs(eventsLogger)
                prop("meterRegistry").isNull()
            }
        }
    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert without retry policy but with meters`() = testDispatcherProvider.runTest {
        // given
        val spec = CassandraSearchStepSpecificationImpl<Any>()
        spec.also {
            it.name = "my-step"
            it.serversConfig = CassandraServerConfiguration(
                servers = listOf("localhost:7777"),
                keyspace = "test_keyspace",
                datacenterProfile = DriverProfile.LOCAL,
                datacenterName = "test_datacenter"
            )
            it.parametersFactory = paramsFactory
            it.queryFactory = queryFactory
            it.monitoring {
                meters = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<CassandraSearchStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).isInstanceOf(CassandraSearchStep::class).all {
            prop("name").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("queryFactory").isEqualTo(queryFactory)
            prop("parametersFactory").isEqualTo(paramsFactory)
            prop("converter").isNotNull().isInstanceOf(CassandraResultSetBatchRecordConverter::class)
            prop("cassandraQueryClient").isNotNull().isInstanceOf(CassandraQueryClientImpl::class).all {
                prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                prop("eventsLogger").isNull()
                prop("meterRegistry").isSameAs(meterRegistry)
            }
        }
    }

}
