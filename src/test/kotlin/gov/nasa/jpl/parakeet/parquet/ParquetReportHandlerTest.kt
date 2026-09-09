package gov.nasa.jpl.parakeet.parquet

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelMetadata
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.Metadatum
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.modules.SerializersModule
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.io.readParquet
import org.jetbrains.kotlinx.dataframe.name
import org.jetbrains.kotlinx.dataframe.type
import kotlin.io.path.createTempDirectory
import kotlin.io.path.div
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.reflect.typeOf
import kotlin.test.Test
import kotlin.test.assertEquals

object ParquetReportHandlerTest {
    private val SERIALIZERS_MODULE = SerializersModule {
        // TODO
    }

    /**
     * To start, drive the report handler directly, without a simulator.
     * This lets us test very specific use cases with minimal dependencies.
     */
    class DirectTests {
        @Test
        fun `a parquet report handler shall write an empty report file when used without initializing any channels`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path, SERIALIZERS_MODULE).use { parquetReportHandler -> }

            assert(path.exists())
            // Even an "empty" report file has some metadata.
            assert(path.fileSize() > 0)
            // Limit found empirically, meant to ensure we're writing a minimal file.
            // If later updates add metadata or something that goes over this limit, it may be appropriate to increase it.
            assert(path.fileSize() < 1024)
        }

        @Test
        fun `a parquet report handler shall include a common timestamp column`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path, SERIALIZERS_MODULE).use { parquetReportHandler -> }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 1, df.shape())
            val column = df.columns().single()
            assertEquals("timestamp", column.name)
            assertEquals(typeOf<LocalDateTime>(), column.type)
        }

        private inline fun <reified T> ChannelizedReportHandler.initChannel(
            name: String,
            metadata: Map<String, Metadatum> = mapOf()
        ) = initChannel<T>(Name(name), metadata)

        private inline fun <reified T> ChannelizedReportHandler.initChannel(
            name: Name,
            metadata: Map<String, Metadatum> = mapOf()
        ) = initChannel(
            ChannelMetadata<T>(
                name,
                metadata,
                typeOf<T>(),
                typeOf<ChannelData<T>>(),
                typeOf<ChannelMetadata<T>>()
            )
        )

    }

    private fun DataFrame<*>.shape(): Pair<Int, Int> = rowsCount() to columnsCount()
}