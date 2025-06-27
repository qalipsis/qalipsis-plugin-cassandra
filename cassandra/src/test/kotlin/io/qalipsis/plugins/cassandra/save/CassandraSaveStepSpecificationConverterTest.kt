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
import io.mockk.spyk
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration
import io.qalipsis.plugins.cassandra.configuration.DriverProfile
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 *
 * @author Svetlana Paliashchuk
 */
@Suppress("UNCHECKED_CAST")
@WithMockk
internal class CassandraSaveStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<CassandraSaveStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

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
    fun `should convert with retry policy and events only`() = testDispatcherProvider.runTest {
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
            prop("name").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNotNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("tableName").isEqualTo(tableName)
            prop("columns").isEqualTo(columns)
            prop("rowsFactory").isEqualTo(rowsFactory)
            prop("cassandraSaveQueryClient").isNotNull().isInstanceOf(CassandraSaveQueryClientImpl::class).all {
                prop("eventsLogger").isSameAs(eventsLogger)
                prop("meterRegistry").isNull()
            }
        }
    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert without retry policy but with meters`() = testDispatcherProvider.runTest {
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
            prop("name").isNotNull().isEqualTo("my-step")
            prop("retryPolicy").isNull()
            prop("sessionBuilder").isNotNull().isInstanceOf(CqlSessionBuilder::class)
            prop("tableName").isEqualTo(tableName)
            prop("columns").isEqualTo(columns)
            prop("rowsFactory").isEqualTo(rowsFactory)
            prop("cassandraSaveQueryClient").isNotNull().isInstanceOf(CassandraSaveQueryClientImpl::class).all {
                prop("eventsLogger").isNull()
                prop("meterRegistry").isSameAs(meterRegistry)
            }
        }
    }
}
