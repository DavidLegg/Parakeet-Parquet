package gov.nasa.jpl.parakeet.parquet

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelMetadata
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.Metadatum
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import gov.nasa.jpl.parakeet.parquet.TestUtils.checkDataFrame
import gov.nasa.jpl.parakeet.parquet.TestUtils.component6
import gov.nasa.jpl.parakeet.parquet.TestUtils.component7
import gov.nasa.jpl.parakeet.parquet.TestUtils.rowEquals
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.io.readParquet
import org.jetbrains.kotlinx.dataframe.name
import org.jetbrains.kotlinx.dataframe.type
import org.jetbrains.kotlinx.dataframe.typeClass
import kotlin.io.path.createTempDirectory
import kotlin.io.path.div
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.reflect.typeOf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.days
import kotlin.time.Instant

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

            ParquetReportHandler(path).use { parquetReportHandler -> }

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

            ParquetReportHandler(path).use { parquetReportHandler -> }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 1, df.shape())
            val column = df.columns().single()
            assertEquals("timestamp", column.name)
            assertEquals(typeOf<LocalDateTime>(), column.type)
        }

        @Test
        fun `a parquet report handler shall include initialized channels as separate columns`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<Int>("int_channel")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 2, df.shape())
            val (timestampCol, intCol) = df.columns()
            assertEquals("timestamp", timestampCol.name)
            assertEquals(typeOf<LocalDateTime>(), timestampCol.type)
            assertEquals("int_channel", intCol.name)
            assertEquals(typeOf<Int>(), intCol.type)
        }

        @Test
        fun `a parquet report handler shall use init order for parquet column order`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<Int>("int_channel_1")
                parquetReportHandler.initChannel<Int>("int_channel_2")
                parquetReportHandler.initChannel<Int>("int_channel_3")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 4, df.shape())
            val (timestampCol, intCol1, intCol2, intCol3) = df.columns()
            assertEquals("timestamp", timestampCol.name)
            assertEquals(typeOf<LocalDateTime>(), timestampCol.type)
            assertEquals("int_channel_1", intCol1.name)
            assertEquals(typeOf<Int>(), intCol1.type)
            assertEquals("int_channel_2", intCol2.name)
            assertEquals(typeOf<Int>(), intCol2.type)
            assertEquals("int_channel_3", intCol3.name)
            assertEquals(typeOf<Int>(), intCol3.type)
        }

        @Test
        fun `a parquet report handler shall choose appropriate column types for primitive channels`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<Int>("int_channel")
                parquetReportHandler.initChannel<Long>("long_channel")
                parquetReportHandler.initChannel<Float>("float_channel")
                parquetReportHandler.initChannel<Double>("double_channel")
                parquetReportHandler.initChannel<Boolean>("boolean_channel")
                parquetReportHandler.initChannel<String>("string_channel")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 7, df.shape())
            val (timestampCol, intCol, longCol, floatCol, doubleCol, booleanCol, stringCol) = df.columns()
            assertEquals("timestamp", timestampCol.name)
            assertEquals(typeOf<LocalDateTime>(), timestampCol.type)
            assertEquals("int_channel", intCol.name)
            assertEquals(typeOf<Int>(), intCol.type)
            assertEquals("long_channel", longCol.name)
            assertEquals(typeOf<Long>(), longCol.type)
            assertEquals("float_channel", floatCol.name)
            assertEquals(typeOf<Float>(), floatCol.type)
            assertEquals("double_channel", doubleCol.name)
            assertEquals(typeOf<Double>(), doubleCol.type)
            assertEquals("boolean_channel", booleanCol.name)
            assertEquals(typeOf<Boolean>(), booleanCol.type)
            assertEquals("string_channel", stringCol.name)
            assertEquals(typeOf<String>(), stringCol.type)
        }

        @Serializable
        data class TestRecord(
            val i: Int,
            val l: Long,
            val f: Float,
            val d: Double,
            val b: Boolean,
            val s: String,
        )

        @Test
        fun `a parquet report handler shall choose appropriate group types for record channels`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<TestRecord>("record_channel")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 2, df.shape())
            val (timestampColumn, recordColumn) = df.columns()
            assertEquals("timestamp", timestampColumn.name)
            assertEquals(typeOf<LocalDateTime>(), timestampColumn.type)
            assertEquals("record_channel", recordColumn.name)
            assertIs<ColumnGroup<*>>(recordColumn)
            val (i, l, f, d, b, s) = recordColumn.columns()
            assertEquals("i", i.name)
            assertEquals(typeOf<Int>(), i.type)
            assertEquals("l", l.name)
            assertEquals(typeOf<Long>(), l.type)
            assertEquals("f", f.name)
            assertEquals(typeOf<Float>(), f.type)
            assertEquals("d", d.name)
            assertEquals(typeOf<Double>(), d.type)
            assertEquals("b", b.name)
            assertEquals(typeOf<Boolean>(), b.type)
            assertEquals("s", s.name)
            assertEquals(typeOf<String>(), s.type)
        }

        @Test
        fun `a parquet report handler shall choose appropriate list types for list channels`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<List<Int>>("list_channel")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 2, df.shape())
            val (timestampCol, listCol) = df.columns()
            assertEquals("timestamp", timestampCol.name)
            assertEquals(typeOf<LocalDateTime>(), timestampCol.type)
            assertEquals("list_channel", listCol.name)
            // Kotlin's DataFrame library currently can't read the parquet schema completely.
            // List element types are lost for an empty parquet file.
            // At the time of writing, the schema was manually verified to be correct:
            // optional group list_channel (LIST) {
            //   repeated group list {
            //     required int32 element;
            //   }
            // }
            assertEquals(List::class, listCol.typeClass)
        }

        @Test
        fun `a parquet report handler shall choose appropriate map types for map channels`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            ParquetReportHandler(path).use { parquetReportHandler ->
                parquetReportHandler.initChannel<Map<String, Int>>("map_channel")
            }

            val df = DataFrame.readParquet(path)
            assertEquals(0 to 2, df.shape())
            val (timestampCol, mapCol) = df.columns()
            assertEquals("timestamp", timestampCol.name)
            assertEquals(typeOf<LocalDateTime>(), timestampCol.type)
            assertEquals("map_channel", mapCol.name)
            // Manually verified at the time of writing that schema is:
            // optional group map_channel (MAP) {
            //   repeated group key_value {
            //     required binary key (STRING);
            //     required int32 value;
            //   }
            // }
            // When run through Kotlin's DataFrame library, this becomes a "frame column" but the contents of the schema are lost.
            // I suspect the DataFrame library is hoping to infer common keys among the entries and shred the maps into columns.
            assertIs<FrameColumn<*>>(mapCol)
        }

        @Test
        fun `a parquet report handler shall include each primitive datum as a row in the parquet file`() {
            val directory = createTempDirectory("ParquetReportHandlerTest_")
            val path = directory / "test.parquet"
            assert(!path.exists())

            val t1 = Instant.parse("2000-01-01T00:00:00Z")
            val t2 = t1 + 1.days
            val t3 = t2 + 1.days
            val t4 = t3 + 1.days
            val t5 = t4 + 1.days
            ParquetReportHandler(path).use { parquetReportHandler ->
                val intChannel = parquetReportHandler.initChannel<Int>("int_channel")
                intChannel.report(t1, 1)
                intChannel.report(t2, 2)
                intChannel.report(t3, 3)
                intChannel.report(t4, 4)
                intChannel.report(t5, 5)
            }

            val df = DataFrame.readParquet(path)
            assertEquals(5 to 2, df.shape())

            checkDataFrame(df, "timestamp", "int_channel") {
                rowEquals(t1.toLocalDateTime(TimeZone.UTC), 1)
                rowEquals(t2.toLocalDateTime(TimeZone.UTC), 2)
                rowEquals(t3.toLocalDateTime(TimeZone.UTC), 3)
                rowEquals(t4.toLocalDateTime(TimeZone.UTC), 4)
                rowEquals(t5.toLocalDateTime(TimeZone.UTC), 5)
            }
        }

        private inline fun <reified T> ChannelizedReportHandler.initChannel(
            name: String,
            metadata: Map<String, Metadatum> = mapOf()
        ) = initChannel<T>(Name(name), metadata)

        private inline fun <reified T> ChannelizedReportHandler.initChannel(
            name: Name,
            metadata: Map<String, Metadatum> = mapOf()
        ): TestChannel<T> {
            initChannel(
                ChannelMetadata<T>(
                    name,
                    metadata,
                    typeOf<T>(),
                    typeOf<ChannelData<T>>(),
                    typeOf<ChannelMetadata<T>>()
                )
            )
            // Re-use the name automatically, to reduce test boilerplate
            return TestChannel { time, value ->
                report(ChannelData(name, time, value))
            }
        }

        /** A version of a channel outside of simulation, for [DirectTests] only. */
        fun interface TestChannel<T> {
            fun report(time: Instant, value: T)
        }
    }

    private fun DataFrame<*>.shape(): Pair<Int, Int> = rowsCount() to columnsCount()
}