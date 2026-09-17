package gov.nasa.jpl.parakeet.arrow

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.elementDescriptors
import kotlinx.serialization.descriptors.elementNames
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TimeStampNanoVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.UInt2Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.writer.BaseWriter
import org.apache.arrow.vector.complex.writer.BitWriter
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import java.io.OutputStream

fun <R> OutputStream.usingArrowStreamReportHandler(
    serializersModule: SerializersModule = Json.serializersModule,
    allocator: BufferAllocator = RootAllocator(),
    block: (ArrowStreamReportHandler) -> R,
) = ArrowStreamReportHandler(this, serializersModule, allocator).use(block)

/**
 * Writes channelized reports from a simulator directly to an Apache Arrow IPC stream.
 */
class ArrowStreamReportHandler(
    private val outputStream: OutputStream,
    private val serializersModule: SerializersModule = SerializersModule {},
    private val allocator: BufferAllocator = RootAllocator(),
    // TODO: Find a smarter way to decide on batch size, e.g. a fixed amount of memory
    private val maxRowsPerBatch: Int = 1024,
) : ChannelizedReportHandler, AutoCloseable {
    private data class ChannelInfo(
        val serializer: KSerializer<*>,
        val field: Field,
        val vector: FieldVector,
        val encoder: ArrowEncoder
    )
    private val timestampField = Field("timestamp", FieldType(false, ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null, null), null)
    private val timestampVector = (timestampField.createVector(allocator) as TimeStampNanoVector).apply {
        setInitialCapacity(maxRowsPerBatch)
        allocateNew()
    }
    private val channelInfo: MutableMap<Name, ChannelInfo> = mutableMapOf()

    private var vectorSchemaRoot: VectorSchemaRoot? = null
    private var writer: ArrowStreamWriter? = null

    private val initialReports: MutableList<ChannelData<*>> = mutableListOf()
    private var rowIndex = 0

    private val initialized get() = writer != null
    private fun initialize() {
        println("DEBUG: Initializing")
        vectorSchemaRoot = VectorSchemaRoot(listOf(timestampVector) + channelInfo.values.map { it.vector })
        writer = ArrowStreamWriter(vectorSchemaRoot, null, outputStream)
        println("DEBUG: Starting writer")
        writer!!.start()
        // Now that we have a writer, re-report all the initial reports to apply the combination policy to them.
        initialReports.forEach { report(it) }
        initialReports.clear()
    }

    private fun flushBatch() {
        println("DEBUG: Flushing batch")
        // Mark all vectors as complete by setting valueCount on them
//        println("DEBUG: Finalizing vectors")
//        channelInfo.values.forEach { it.vector.valueCount = rowIndex }
        println("DEBUG: Finalizing VSR")
        vectorSchemaRoot!!.setRowCount(rowIndex)
        // Ask the writer to write all vectors to the output stream
        println("DEBUG: Writing batch")
        writer!!.writeBatch()
        // Finally, reset all vectors for the next batch
        println("DEBUG: Resetting vectors")
        channelInfo.values.forEach { it.vector.reset() }
        rowIndex = 0
    }

    private var closed = false
    override fun close() {
        println("DEBUG: Closing")
        if (!initialized) initialize()
        println("DEBUG: Close: rowIndex = $rowIndex")
        if (rowIndex > 0) flushBatch()

        // Close things in the opposite order of how we opened them
        // TODO: Should we wrap any of this in try/catch/finally?
        println("DEBUG: Closing writer")
        writer!!.close()
        println("DEBUG: Closing VSR")
        vectorSchemaRoot!!.close()
        println("DEBUG: Closing vectors")
        channelInfo.values.forEach { it.vector.close() }
        println("DEBUG: Closed")
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
        println("DEBUG: Creating vector for $nameString")
        val vector = field.createVector(allocator)
        println("DEBUG: Setting capacity for $nameString vector to $maxRowsPerBatch")
        vector.setInitialCapacity(maxRowsPerBatch)
        println("DEBUG: Allocating vector for $nameString")
        vector.allocateNew()
        channelInfo[name] = ChannelInfo(
            serializer,
            field,
            vector,
            ArrowEncoder(vector, serializersModule)
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

        // TODO: Support combining rows
        val channelInfo = channelInfo.getValue(data.channel)
        println("DEBUG: Writing row $rowIndex timestamp ${data.time}")
        timestampVector.setSafe(rowIndex, data.time.epochSeconds * 1_000_000_000L + data.time.nanosecondsOfSecond)
        println("DEBUG: Writing row $rowIndex channel ${data.channel} value ${data.data}")
        @Suppress("UNCHECKED_CAST")
        (channelInfo.serializer as KSerializer<Any?>).serialize(channelInfo.encoder, data.data)
        rowIndex++
        if (rowIndex >= maxRowsPerBatch) flushBatch()
    }

    // TODO: I think the encoder needs to be re-implemented using writers...
    //   Peeking at the way the UnionListWriter works, it looks hard to replicate correctly.

    private class ArrowEncoder(
        private val vector: FieldVector,
        override val serializersModule: SerializersModule,
    ) : Encoder {
        var rowIndex = 0

        @ExperimentalSerializationApi
        override fun encodeNull() {
            vector.setNull(rowIndex)
        }

        override fun encodeBoolean(value: Boolean) {
            (vector as BitVector).setSafe(rowIndex, if (value) 1 else 0)
        }

        override fun encodeByte(value: Byte) {
            (vector as TinyIntVector).setSafe(rowIndex, value)
        }

        override fun encodeShort(value: Short) {
            (vector as SmallIntVector).setSafe(rowIndex, value)
        }

        override fun encodeChar(value: Char) {
            (vector as UInt2Vector).setSafe(rowIndex, value)
        }

        override fun encodeInt(value: Int) {
            (vector as IntVector).setSafe(rowIndex, value)
        }

        override fun encodeLong(value: Long) {
            (vector as BigIntVector).setSafe(rowIndex, value)
        }

        override fun encodeFloat(value: Float) {
            (vector as Float4Vector).setSafe(rowIndex, value)
        }

        override fun encodeDouble(value: Double) {
            (vector as Float8Vector).setSafe(rowIndex, value)
        }

        override fun encodeString(value: String) {
            (vector as VarCharVector).setSafe(rowIndex, value.encodeToByteArray())
        }

        override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
            // TODO: Update this once we use a dictionary encoding for enums
            encodeString(enumDescriptor.getElementName(index))
        }

        override fun encodeInline(descriptor: SerialDescriptor): Encoder {
            return this
        }

        override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
            return when (descriptor.kind as StructureKind) {
                StructureKind.CLASS -> createClassEncoder(descriptor)
                StructureKind.LIST -> TODO()
                StructureKind.MAP -> TODO()
                StructureKind.OBJECT -> TODO()
            }
        }

        @OptIn(ExperimentalSerializationApi::class)
        private fun createClassEncoder(descriptor: SerialDescriptor): CompositeEncoder {
            vector as StructVector
            return object : CompositeEncoder {
                override val serializersModule: SerializersModule = this@ArrowEncoder.serializersModule
                private val children = vector.childrenFromFields.map { ArrowEncoder(it, serializersModule) }

                override fun endStructure(descriptor: SerialDescriptor) {
                    vector.setIndexDefined(rowIndex)
                }

                override fun encodeBooleanElement(descriptor: SerialDescriptor, index: Int, value: Boolean) {
                    children[index].encodeBoolean(value)
                }

                override fun encodeByteElement(descriptor: SerialDescriptor, index: Int, value: Byte) {
                    children[index].encodeByte(value)
                }

                override fun encodeCharElement(descriptor: SerialDescriptor, index: Int, value: Char) {
                    children[index].encodeChar(value)
                }

                override fun encodeDoubleElement(descriptor: SerialDescriptor, index: Int, value: Double) {
                    children[index].encodeDouble(value)
                }

                override fun encodeFloatElement(descriptor: SerialDescriptor, index: Int, value: Float) {
                    children[index].encodeFloat(value)
                }

                override fun encodeInlineElement(descriptor: SerialDescriptor, index: Int): Encoder {
                    return children[index]
                }

                override fun encodeIntElement(descriptor: SerialDescriptor, index: Int, value: Int) {
                    children[index].encodeInt(value)
                }

                override fun encodeLongElement(descriptor: SerialDescriptor, index: Int, value: Long) {
                    children[index].encodeLong(value)
                }

                override fun encodeShortElement(descriptor: SerialDescriptor, index: Int, value: Short) {
                    children[index].encodeShort(value)
                }

                override fun encodeStringElement(descriptor: SerialDescriptor, index: Int, value: String) {
                    children[index].encodeString(value)
                }

                override fun <T> encodeSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T) {
                    children[index].encodeSerializableValue(serializer, value)
                }

                override fun <T : Any> encodeNullableSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T?) {
                    if (value == null) {
                        children[index].encodeNull()
                    } else {
                        children[index].encodeSerializableValue(serializer, value)
                    }
                }
            }
        }

        private fun createListEncoder(descriptor: SerialDescriptor): CompositeEncoder {
            return object : CompositeEncoder {
                override val serializersModule: SerializersModule = this@ArrowEncoder.serializersModule

                override fun endStructure(descriptor: SerialDescriptor) {
                    TODO("Not yet implemented")
                }

                override fun encodeBooleanElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Boolean
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeByteElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Byte
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeShortElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Short
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeCharElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Char
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeIntElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Int
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeLongElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Long
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeFloatElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Float
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeDoubleElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: Double
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeStringElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    value: String
                ) {
                    TODO("Not yet implemented")
                }

                override fun encodeInlineElement(
                    descriptor: SerialDescriptor,
                    index: Int
                ): Encoder {
                    TODO("Not yet implemented")
                }

                override fun <T> encodeSerializableElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    serializer: SerializationStrategy<T>,
                    value: T
                ) {
                    TODO("Not yet implemented")
                }

                @ExperimentalSerializationApi
                override fun <T : Any> encodeNullableSerializableElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    serializer: SerializationStrategy<T>,
                    value: T?
                ) {
                    TODO("Not yet implemented")
                }
            }
        }
    }
}
