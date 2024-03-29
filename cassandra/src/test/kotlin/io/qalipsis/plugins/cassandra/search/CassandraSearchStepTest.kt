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

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.mockk
import io.mockk.spyk
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetConverter
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 *
 * @author Gabriel Moraes
 */
@WithMockk
internal class CassandraSearchStepTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val queryFactory: (suspend (ctx: StepContext<*, *>, input: Any?) -> String) = relaxedMockk()

    private val paramsFactory: (suspend (ctx: StepContext<*, *>, input: Any) -> List<Any>) = relaxedMockk()

    @RelaxedMockK
    private lateinit var cqlBuilder: CqlSessionBuilder

    @RelaxedMockK
    private lateinit var retryPolicy: RetryPolicy

    @RelaxedMockK
    private lateinit var converter: CassandraResultSetConverter<CassandraQueryResult, Any, Any>

    @RelaxedMockK
    private lateinit var cassandraQueryClient: CassandraQueryClient

    @RelaxedMockK
    private lateinit var context: StepContext<Any, Pair<Any, List<CassandraRecord<Map<CqlIdentifier, Any?>>>>>

    private lateinit var cassandraSearchStep: CassandraSearchStep<Any>

    @BeforeEach
    fun setUp() = testDispatcherProvider.runTest {
        val completionStageSession = spyk<CompletionStage<CqlSession>>(CompletableFuture())
        completionStageSession.toCompletableFuture().complete(mockk())
        every { cqlBuilder.buildAsync() } returns completionStageSession

        cassandraSearchStep = CassandraSearchStep(
            id = "my-step",
            retryPolicy = retryPolicy,
            sessionBuilder = cqlBuilder,
            queryFactory = queryFactory,
            parametersFactory = paramsFactory,
            converter = converter,
            cassandraQueryClient = cassandraQueryClient
        )

        cassandraSearchStep.start(mockk())
    }

    @Test
    fun `should execute query with success`() = testDispatcherProvider.runTest {
        val cassandraSearchReturn: CassandraQueryResult = mockk()
        coEvery { queryFactory.invoke(any(), any()) } returns "SELECT * FROM TRACKER WHERE ID = ?"
        coEvery { paramsFactory.invoke(any(), any()) } returns listOf(42)
        coEvery {
            cassandraQueryClient.execute(
                any(),
                any(),
                any(),
                context.toEventTags()
            )
        } returns cassandraSearchReturn

        cassandraSearchStep.execute(context)

        coVerify {
            cassandraQueryClient.execute(any(), "SELECT * FROM TRACKER WHERE ID = ?", listOf(42), context.toEventTags())
            converter.supply(any(), cassandraSearchReturn, any(), any())
        }
    }

}
