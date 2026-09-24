package gov.nasa.jpl.parakeet.arrow

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import gov.nasa.jpl.parakeet.parquet.CombineReportsRule
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.elementNames
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.TimeStampNanoVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import java.io.OutputStream
import kotlin.time.Instant

fun <R> OutputStream.usingArrowStreamReportHandler(
    serializersModule: SerializersModule = Json.serializersModule,
    combineReportsRule: CombineReportsRule = CombineReportsRule.DONT_COMBINE,
    allocator: BufferAllocator = RootAllocator(),
    block: (ArrowStreamReportHandler) -> R,
) = ArrowStreamReportHandler(this, serializersModule, combineReportsRule, allocator).use(block)

/**
 * Writes channelized reports from a simulator directly to an Apache Arrow IPC stream.
 */
class ArrowStreamReportHandler(
    private val outputStream: OutputStream,
    private val serializersModule: SerializersModule = SerializersModule {},
    private val combineReportsRule: CombineReportsRule = CombineReportsRule.DONT_COMBINE,
    private val allocator: BufferAllocator = RootAllocator(),
    // TODO: Find a smarter way to decide on batch size, e.g. a fixed amount of memory
    private val maxRowsPerBatch: Int = 100_000,
) : ChannelizedReportHandler, AutoCloseable {
    private data class ChannelInfo(
        val serializer: KSerializer<*>,
        val field: Field,
        val vector: FieldVector,
        val encoder: ArrowEncoder
    )
    private val timestampField = Field("timestamp", FieldType(false, ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null, null), null)
    private val timestampVector = timestampField.createVector(allocator) as TimeStampNanoVector
    private val channelInfo: MutableMap<Name, ChannelInfo> = mutableMapOf()

    private var vectorSchemaRoot: VectorSchemaRoot? = null
    private var writer: ArrowStreamWriter? = null

    private val initialReports: MutableList<ChannelData<*>> = mutableListOf()
    private var lastWrittenTime: Instant = Instant.DISTANT_PAST
    private var lastWrittenRowIndex = -1

    private val initialized get() = writer != null
    private fun initialize() {
        val allVectors = listOf(timestampVector) + channelInfo.values.map { it.vector }
        // TODO: If we switch to memory-based sizing, we can calculate the number of rows here
        allVectors.forEach { it.setInitialCapacity(maxRowsPerBatch) }
        vectorSchemaRoot = VectorSchemaRoot(allVectors)
        vectorSchemaRoot!!.allocateNew()
        writer = ArrowStreamWriter(vectorSchemaRoot, null, outputStream)
        writer!!.start()
        // Now that we have a writer, re-report all the initial reports to apply the combination policy to them.
        initialReports.forEach { report(it) }
        initialReports.clear()
    }

    private fun flushBatch() {
        // Rows are indexed 0-based, so the number of completed rows is the last completed row index + 1
        val completedRows = lastWrittenRowIndex + 1
        // Mark all vectors as complete by setting valueCount on them
        channelInfo.values.forEach { it.vector.valueCount = completedRows }
        vectorSchemaRoot!!.setRowCount(completedRows)
        // Ask the writer to write all vectors to the output stream
        writer!!.writeBatch()
        // Finally, reset all vectors for the next batch
        channelInfo.values.forEach { it.vector.reset() }
        lastWrittenRowIndex = -1
    }

    private var closed = false
    override fun close() {
        if (!initialized) initialize()
        if (lastWrittenRowIndex >= 0) flushBatch()

        // Close things in the opposite order of how we opened them
        // TODO: Should we wrap any of this in try/catch/finally?
        writer!!.close()
        vectorSchemaRoot!!.close()
        channelInfo.values.forEach { it.vector.close() }
        closed = true
    }

    override fun <T> initChannel(metadata: ChannelReport.ChannelMetadata<T>) {
        check(!closed) {
            "Attempting to use a closed ${this::class.simpleName}"
        }
        check(!initialized) {
            "Cannot initialize a channel on a report handler that has already been fully initialized"
        }
        val name = metadata.channel
        require(name !in channelInfo) {
            "Channel $name has already been initialized"
        }
        val serializer = serializersModule.serializer(metadata.dataType)
        if (serializer.descriptor.isNullable) {
            System.err.println("Channel $name is nullable. Since null is used to indicate lack of a report at that time, null reports will be indistinguishable from lack of a report.")
        }

        val nameString = name.toString()
        val field = serializer.descriptor.toArrowField(nameString, topLevel = true)
        val vector = field.createVector(allocator)
        vector.setInitialCapacity(maxRowsPerBatch)
        vector.allocateNew()
        channelInfo[name] = ChannelInfo(
            serializer,
            field,
            vector,
            ArrowEncoder(vector.minorType.getNewFieldWriter(vector), serializersModule),
        )
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun SerialDescriptor.toArrowField(name: String, topLevel: Boolean = false): Field {
        // TODO: Include channel metadata?
        val nullable = isNullable || topLevel
        fun primitive(type: ArrowType.PrimitiveType) =
            Field(name, FieldType(nullable, type, null, null), null)

        return when (kind) {
            // For primitives, just convert to the most similar Arrow primitive type
            PrimitiveKind.BOOLEAN -> primitive(ArrowType.Bool.INSTANCE)
            PrimitiveKind.DOUBLE -> primitive(ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
            PrimitiveKind.FLOAT -> primitive(ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
            PrimitiveKind.INT -> primitive(ArrowType.Int(32, true))
            PrimitiveKind.LONG -> primitive(ArrowType.Int(64, true))
            PrimitiveKind.SHORT -> primitive(ArrowType.Int(16, true))
            PrimitiveKind.BYTE -> primitive(ArrowType.Int(8, true))
            PrimitiveKind.CHAR -> primitive(ArrowType.Int(16, false))
            // There is also a Utf8View type, but it's not clear to me how to use that, or if it's appropriate...
            PrimitiveKind.STRING -> primitive(ArrowType.Utf8.INSTANCE)

            // TODO: We should dictionary-encode enums, but for now let's just treat them like strings
            SerialKind.ENUM -> primitive(ArrowType.Utf8.INSTANCE)

            StructureKind.LIST -> Field(name, FieldType(nullable, ArrowType.List.INSTANCE, null, null), listOf(
                // While Arrow doesn't mandate the name of the child field here, using "element" is consistent with Parquet's schema
                getElementDescriptor(0).toArrowField("element")
            ))

            StructureKind.MAP -> Field(name, FieldType(nullable, ArrowType.Map(false), null, null), listOf(
                // Arrow mandates that maps have a single child field, which must be a struct with two fields
                Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), listOf(
                    // While not required by Arrow, using "key" and "value" as the field names is consistent with Parquet's schema
                    getElementDescriptor(0).toArrowField("key"),
                    getElementDescriptor(1).toArrowField("value")
                ))
            ))

            StructureKind.CLASS -> if (isInline) {
                // Special case: inline classes are serialized as their underlying type, but with the top-level name
                require(elementsCount == 1) { "Inline classes must have exactly one element" }
                getElementDescriptor(0).toArrowField(name, topLevel = topLevel)
            } else {
                // General case: just use a struct with all the fields as children
                Field(
                    name, FieldType(nullable, ArrowType.Struct.INSTANCE, null, null),
                    (elementNames zip elementDescriptors).map { (name, descriptor) -> descriptor.toArrowField(name) })
            }

            // TODO: OBJECT represents singletons that don't serialize to anything... should they be skipped? errored? unclear...
            StructureKind.OBJECT ->
                throw NotImplementedError("Objects are not currently supported for Parquet serialization")

            // Since we should be looking at a fully-reified type, I don't think these serial kinds should be reachable.
            // TODO: make this a more informative error
            SerialKind.CONTEXTUAL, PolymorphicKind.OPEN, PolymorphicKind.SEALED ->
                throw IllegalArgumentException("Only concrete types are supported for Arrow IPC serialization")
        }
    }

    override fun <T> report(data: ChannelData<T>) {
        check(!closed) {
            "Attempting to use a closed ${this::class.simpleName}"
        }
        if (!initialized) {
            if (initialReports.isEmpty() || initialReports.first().time == data.time) {
                // This is (potentially) an initial report, so buffer it until time progresses
                initialReports.add(data)
                return
            } else {
                // This is the first certainly-not-initial report, so all channels are now initialized
                // Initialize the writer and flush the initial reports
                initialize()
                // Then fall through to the general case
            }
        }

        val channelInfo = channelInfo.getValue(data.channel)

        // Decide whether to advance to the next row based on our report-combining rule.
        val shouldAdvanceRow = when (combineReportsRule) {
            CombineReportsRule.DONT_COMBINE -> {
                // Advance to the next row unconditionally
                true
            }
            CombineReportsRule.COMBINE_AND_KEEP_ALL -> {
                // Advance to the next row when time changes or the slot we would write to is already full
                data.time != lastWrittenTime || !channelInfo.vector.isNull(lastWrittenRowIndex)
            }
            CombineReportsRule.COMBINE_AND_KEEP_LAST -> {
                // Only advance to the next row when time changes (permits overwriting a filled slot)
                data.time != lastWrittenTime
            }
        }
        // Act on that decision
        if (shouldAdvanceRow) {
            // If the batch is already full, flush it.
            if (lastWrittenRowIndex >= maxRowsPerBatch - 1) {
                flushBatch()
            }
            // At this point, the row index points to the last completed row (or -1 if we just flushed a batch).
            lastWrittenRowIndex++
            // At this point, the row index points to an empty row. Record that time.
            timestampVector.setSafe(lastWrittenRowIndex, data.time.epochSeconds * 1_000_000_000L + data.time.nanosecondsOfSecond)
            lastWrittenTime = data.time
        }

        // At this point, lastWrittenRowIndex points to the row we should write to,
        // regardless of whether that's writing to an empty slot or overwriting old data.
        channelInfo.encoder.position = lastWrittenRowIndex
        @Suppress("UNCHECKED_CAST")
        (channelInfo.serializer as KSerializer<Any?>).serialize(channelInfo.encoder, data.data)
    }
}
