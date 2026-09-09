package gov.nasa.jpl.parakeet.parquet

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.nio.file.Path

class ParquetReportHandler(
    private val path: Path,
    private val serializersModule: SerializersModule = Json.serializersModule,
) : ChannelizedReportHandler, AutoCloseable {
    private data class ChannelInfo(
        val index: Int,
        val type: Type,
        val serializer: KSerializer<*>,
    )

    private val channelInfo: MutableMap<Name, ChannelInfo> = mutableMapOf()
    private val initialReports: MutableList<ChannelData<*>> = mutableListOf()

    private var writer: ParquetWriter<ChannelData<*>>? = null

    private val initialized: Boolean get() = writer != null
    private fun initialize() {
        // Lock in our schema and build our writer
        // In so doing, we automatically flip virtual property `initialized` to true
        writer = ChannelDataParquetWriterBuilder(path, serializersModule, channelInfo).build()
        // Flush all initial reports
        initialReports.forEach { writer!!.write(it) }
        initialReports.clear()
    }

    private var closed = false

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
            System.err.println("Channel ${metadata.channel} is nullable. Since null is used to indicate lack of a report at that time, null reports will be indistinguishable from lack of a report.")
        }
        channelInfo[name] = ChannelInfo(
            // Add 1 to account for the timestamp field at index 0
            channelInfo.size + 1,
            // Regardless of whether the original channel is nullable, the parquet column must be nullable to account for missing data.
            serializer.descriptor.nullable.toParquetMessageType(name.toString()),
            serializer,
        )
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

        writer!!.write(data)
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun SerialDescriptor.toParquetMessageType(name: String): Type {
        val repetition = if (isNullable) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED
        fun primitive(type: PrimitiveTypeName) = Types.primitive(type, repetition)

        return when (kind) {
            // For primitives, just convert to the most similar parquet primitive type
            PrimitiveKind.BOOLEAN -> primitive(PrimitiveTypeName.BOOLEAN)
            PrimitiveKind.DOUBLE -> primitive(PrimitiveTypeName.DOUBLE)
            PrimitiveKind.FLOAT -> primitive(PrimitiveTypeName.FLOAT)
            PrimitiveKind.INT -> primitive(PrimitiveTypeName.INT32)
            PrimitiveKind.LONG -> primitive(PrimitiveTypeName.INT64)
            // TODO: Consider if we should do something more intelligent with enums, like using a fixed-length binary based on enum size or something...
            // For now, just treat enums as general strings
            PrimitiveKind.STRING, SerialKind.ENUM -> primitive(PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType())
            // Char, byte, and short don't exactly map over cleanly, but they're rarely used. I think this is a fine mapping.
            PrimitiveKind.CHAR -> primitive(PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.intType(16, false))
            PrimitiveKind.BYTE -> primitive(PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.intType(8))
            PrimitiveKind.SHORT -> primitive(PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.intType(16))

            StructureKind.LIST -> Types.list(repetition)
                // Parquet schema requires that the element name be "element"
                .element(getElementDescriptor(0).toParquetMessageType("element"))

            StructureKind.MAP -> Types.map(repetition)
                // Parquet schema requires that the key and value names be "key" and "value"
                .key(getElementDescriptor(0).toParquetMessageType("key"))
                .value(getElementDescriptor(1).toParquetMessageType("value"))

            StructureKind.CLASS -> Types.buildGroup(repetition).also {
                for ((name, descriptor) in elementNames zip elementDescriptors) {
                    it.addField(descriptor.toParquetMessageType(name))
                }
            }

            // TODO: OBJECT represents singletons that don't serialize to anything... should they be skipped? errored? unclear...
            StructureKind.OBJECT ->
                throw NotImplementedError("Objects are not currently supported for Parquet serialization")

            // Since we should be looking at a fully-reified type, I don't think these serial kinds should be reachable.
            // TODO: make this a more informative error
            SerialKind.CONTEXTUAL, PolymorphicKind.OPEN, PolymorphicKind.SEALED ->
                throw IllegalArgumentException("Only concrete types are supported for Parquet serialization")
        }.named(name)
    }

    override fun close() {
        // Silently tolerate re-closing an already-closed report handler
        if (closed) return
        // In rare cases, we may only see reports at the initial time.
        // In these cases, we must initialize to flush those reports, before we close.
        if (!initialized) initialize()
        // Having asserted that we're initialized, pass on the request to close to our writer.
        writer!!.close()
        // Finally, mark ourselves as closed
        closed = true
    }

    private class ChannelDataParquetWriterBuilder(
        path: Path,
        private val serializersModule: SerializersModule,
        private val channelInfo: Map<Name, ChannelInfo>,
    ) : ParquetWriter.Builder<ChannelData<*>, ChannelDataParquetWriterBuilder>(
        LocalOutputFile(path)
    ) {
        override fun self(): ChannelDataParquetWriterBuilder = this

        @Deprecated("Deprecated in Java")
        override fun getWriteSupport(conf: Configuration?): WriteSupport<ChannelData<*>?> =
            object : WriteSupport<ChannelData<*>?>() {
                private var recordConsumer: RecordConsumer? = null
                private var parquetEncoder: ParquetEncoder? = null

                @Deprecated("Deprecated in Java")
                override fun init(configuration: Configuration?): WriteContext = WriteContext(
                    Types.buildMessage()
                        // Timezone support appears to be less common than I'd like - to keep this highly-compatible,
                        // leave the timezone information off, and assume the reader will know what timezone is appropriate.
                        .addField(Types.required(PrimitiveTypeName.INT64)
                            .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                            .named("timestamp"))
                        .addFields(*channelInfo.values
                            .sortedBy { it.index }
                            .map { it.type }
                            .toTypedArray())
                        .named("root"),
                    mapOf(),
                )

                override fun prepareForWrite(recordConsumer: RecordConsumer?) {
                    this.recordConsumer = recordConsumer
                    this.parquetEncoder = ParquetEncoder(serializersModule, recordConsumer!!)
                }

                override fun write(record: ChannelData<*>?) {
                    recordConsumer!!.apply {
                        startMessage()

                        startField("timestamp", 0)
                        // TODO: Do we need to handle overflows here?
                        addLong(record!!.time.epochSeconds * 1_000_000_000L + record.time.nanosecondsOfSecond)
                        endField("timestamp", 0)

                        val (index, type, serializer) = channelInfo.getValue(record.channel)
                        // Get the name from type to avoid re-computing channel.name.toString(), since string-building can be expensive.
                        startField(type.name, index)
                        // TYPE SAFETY: We're using the serializer for the channel's declared data type.
                        @Suppress("UNCHECKED_CAST")
                        (serializer as KSerializer<Any?>).serialize(parquetEncoder!!, record.data)
                        endField(type.name, index)

                        endMessage()
                    }
                }
            }
    }

    private class ParquetEncoder(
        override val serializersModule: SerializersModule,
        private val recordConsumer: RecordConsumer
    ) : Encoder {
        @ExperimentalSerializationApi
        override fun encodeNull() {
            // Do nothing - null is not encoded in parquet, just leave the field blank
        }

        override fun encodeBoolean(value: Boolean) {
            recordConsumer.addBoolean(value)
        }

        override fun encodeByte(value: Byte) {
            recordConsumer.addInteger(value.toInt())
        }

        override fun encodeShort(value: Short) {
            recordConsumer.addInteger(value.toInt())
        }

        override fun encodeChar(value: Char) {
            recordConsumer.addInteger(value.code)
        }

        override fun encodeInt(value: Int) {
            recordConsumer.addInteger(value)
        }

        override fun encodeLong(value: Long) {
            recordConsumer.addLong(value)
        }

        override fun encodeFloat(value: Float) {
            recordConsumer.addFloat(value)
        }

        override fun encodeDouble(value: Double) {
            recordConsumer.addDouble(value)
        }

        override fun encodeString(value: String) {
            recordConsumer.addBinary(Binary.fromConstantByteArray(value.encodeToByteArray()))
        }

        override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
            encodeString(enumDescriptor.getElementName(index))
        }

        override fun encodeInline(descriptor: SerialDescriptor): Encoder {
            return this
        }

        override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
            recordConsumer.startGroup()
            // Special rules apply for each kind of structure. Define them here.
            val structureKind = descriptor.kind as StructureKind
            var structIsEmpty = true
            var startedMapEntry = false

            fun startField(descriptor: SerialDescriptor, index: Int) = when (structureKind) {
                StructureKind.CLASS -> recordConsumer.startField(descriptor.getElementName(index), index)
                StructureKind.LIST -> {
                    if (structIsEmpty) recordConsumer.startField("list", 0)
                    structIsEmpty = false
                    recordConsumer.startGroup()
                    recordConsumer.startField("element", 0)
                }
                StructureKind.MAP -> {
                    if (structIsEmpty) recordConsumer.startField("key_value", 0)
                    structIsEmpty = false
                    if (index % 2 == 0) {
                        check (!startedMapEntry) {
                            "Map serializers must alternate key, then value"
                        }
                        // Start the group for this map entry
                        recordConsumer.startGroup()
                        recordConsumer.startField("key", 0)
                        startedMapEntry = true
                    } else {
                        check (startedMapEntry) {
                            "Map serializers must alternate key, then value"
                        }
                        recordConsumer.startField("value", 1)
                    }
                }

                StructureKind.OBJECT -> throw AssertionError("Impossible code path")
            }
            fun endField(descriptor: SerialDescriptor, index: Int) = when (structureKind) {
                StructureKind.CLASS -> recordConsumer.endField(descriptor.getElementName(index), index)
                StructureKind.LIST -> {
                    recordConsumer.endField("element", 0)
                    recordConsumer.endGroup()
                }
                StructureKind.MAP -> {
                    if (index % 2 == 0) {
                        recordConsumer.endField("key", 0)
                    } else {
                        recordConsumer.endField("value", 1)
                        // End the group for this map entry
                        recordConsumer.endGroup()
                        startedMapEntry = false
                    }
                }

                StructureKind.OBJECT -> throw AssertionError("Impossible code path")
            }

            return object : CompositeEncoder {
                override val serializersModule: SerializersModule
                    get() = this@ParquetEncoder.serializersModule

                override fun endStructure(descriptor: SerialDescriptor) {
                    when (structureKind) {
                        StructureKind.CLASS -> {/* nothing to do */}
                        StructureKind.LIST -> if (!structIsEmpty) recordConsumer.endField("list", 0)
                        StructureKind.MAP -> if (!structIsEmpty) recordConsumer.endField("key_value", 0)
                        StructureKind.OBJECT -> throw AssertionError("Impossible code path")
                    }
                    recordConsumer.endGroup()
                }

                override fun encodeBooleanElement(descriptor: SerialDescriptor, index: Int, value: Boolean) {
                    startField(descriptor, index)
                    encodeBoolean(value)
                    endField(descriptor, index)
                }

                override fun encodeByteElement(descriptor: SerialDescriptor, index: Int, value: Byte) {
                    startField(descriptor, index)
                    encodeByte(value)
                    endField(descriptor, index)
                }

                override fun encodeShortElement(descriptor: SerialDescriptor, index: Int, value: Short) {
                    startField(descriptor, index)
                    encodeShort(value)
                    endField(descriptor, index)
                }

                override fun encodeCharElement(descriptor: SerialDescriptor, index: Int, value: Char) {
                    startField(descriptor, index)
                    encodeChar(value)
                    endField(descriptor, index)
                }

                override fun encodeIntElement(descriptor: SerialDescriptor, index: Int, value: Int) {
                    startField(descriptor, index)
                    encodeInt(value)
                    endField(descriptor, index)
                }

                override fun encodeLongElement(descriptor: SerialDescriptor, index: Int, value: Long) {
                    startField(descriptor, index)
                    encodeLong(value)
                    endField(descriptor, index)
                }

                override fun encodeFloatElement(descriptor: SerialDescriptor, index: Int, value: Float) {
                    startField(descriptor, index)
                    encodeFloat(value)
                    endField(descriptor, index)
                }

                override fun encodeDoubleElement(descriptor: SerialDescriptor, index: Int, value: Double) {
                    startField(descriptor, index)
                    encodeDouble(value)
                    endField(descriptor, index)
                }

                override fun encodeStringElement(descriptor: SerialDescriptor, index: Int, value: String) {
                    startField(descriptor, index)
                    encodeString(value)
                    endField(descriptor, index)
                }

                override fun encodeInlineElement(descriptor: SerialDescriptor, index: Int): Encoder {
                    return this@ParquetEncoder
                }

                override fun <T> encodeSerializableElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    serializer: SerializationStrategy<T>,
                    value: T,
                ) {
                    startField(descriptor, index)
                    serializer.serialize(this@ParquetEncoder, value)
                    endField(descriptor, index)
                }

                @ExperimentalSerializationApi
                override fun <T : Any> encodeNullableSerializableElement(
                    descriptor: SerialDescriptor,
                    index: Int,
                    serializer: SerializationStrategy<T>,
                    value: T?,
                ) {
                    if (value == null) {
                        when (structureKind) {
                            StructureKind.CLASS -> {/* nothing to do */}
                            StructureKind.LIST -> {
                                // Write a group without the "element" field to represent a null element
                                recordConsumer.startGroup()
                                recordConsumer.endGroup()
                            }
                            StructureKind.MAP -> {
                                require(startedMapEntry && index % 2 == 1) {
                                    "Only values of a map may be null in parquet"
                                }
                                // Record the null value by omitting the value field, ending the key_value group early
                                recordConsumer.endGroup()
                                startedMapEntry = false
                            }
                            StructureKind.OBJECT -> throw AssertionError("Impossible code path")
                        }
                    } else {
                        encodeSerializableElement(descriptor, index, serializer, value)
                    }
                }
            }
        }
    }
}