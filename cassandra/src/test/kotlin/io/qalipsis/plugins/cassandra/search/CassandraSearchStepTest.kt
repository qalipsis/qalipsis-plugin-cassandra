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
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetConverter
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 *
 * @author Gabriel Moraes
 */
@WithMockk
internal class CassandraSearchStepTest {

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
    fun setUp() = runBlockingTest {
        val completionStageSession = spyk<CompletionStage<CqlSession>>(CompletableFuture())
        completionStageSession.toCompletableFuture().complete(mockk())
        val completableFutureSession = spyk(completionStageSession.asSuspended())
        every { cqlBuilder.buildAsync() } returns completionStageSession
        every { completionStageSession.asSuspended() } returns completableFutureSession
        coEvery { completableFutureSession.get(any()) } returns mockk()

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
    fun `should execute query with success`() = runBlockingTest {
        val cassandraSearchReturn: CassandraQueryResult = mockk()
        coEvery { queryFactory.invoke(any(), any()) } returns "SELECT * FROM TRACKER WHERE ID = ?"
        coEvery { paramsFactory.invoke(any(), any()) } returns listOf(42)
        coEvery { cassandraQueryClient.execute(any(), any(), any(), context.toEventTags()) } returns cassandraSearchReturn

        cassandraSearchStep.execute(context)

        coVerify {
            cassandraQueryClient.execute(any(), "SELECT * FROM TRACKER WHERE ID = ?", listOf(42), context.toEventTags())
            converter.supply(any(), cassandraSearchReturn, any(), any())
        }
    }

    @Test
    fun `should throw exception when query is empty`() = runBlockingTest {
        coEvery { queryFactory.invoke(any(), any()) } returns "select * from tracker"
        coEvery { paramsFactory.invoke(any(), any()) } returns listOf(42)

        assertThrows<IllegalArgumentException> {
            cassandraSearchStep.execute(context)
        }

        coVerify(inverse = true) {
            cassandraQueryClient.execute(any(), any(), any(), context.toEventTags())
            converter.supply(any(), any(), any(), any())
        }
    }

    @Test
    fun `should throw exception when parameters list is empty and query has at least one`() = runBlockingTest {
        coEvery { queryFactory.invoke(any(), any()) } returns "SELECT * FROM TRACKER WHERE DEVICE_NAME = ?"
        coEvery { paramsFactory.invoke(any(), any()) } returns emptyList()

        assertThrows<IllegalArgumentException> {
            cassandraSearchStep.execute(context)
        }

        coVerify(inverse = true) {
            cassandraQueryClient.execute(any(), any(), any(), context.toEventTags())
            converter.supply(any(), any(), any(), any())
        }
    }

    @Test
    fun `should execute query when parameters list is empty and query has none`() = runBlockingTest {
        val cassandraSearchReturn: CassandraQueryResult = mockk()
        coEvery { queryFactory.invoke(any(), any()) } returns "SELECT * FROM TRACKER"
        coEvery { paramsFactory.invoke(any(), any()) } returns emptyList()
        coEvery { cassandraQueryClient.execute(any(), any(), any(), context.toEventTags()) } returns cassandraSearchReturn

        cassandraSearchStep.execute(context)

        coVerify {
            cassandraQueryClient.execute(any(), "SELECT * FROM TRACKER", emptyList(), context.toEventTags())
            converter.supply(any(), cassandraSearchReturn, any(), any())
        }
    }
}
