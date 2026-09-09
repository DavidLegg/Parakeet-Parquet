package gov.nasa.jpl.parakeet.parquet

import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelReport.ChannelData
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.kernel.Name
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.nio.file.Path

class ParquetReportHandler(
    private val path: Path,
    private val serializersModule: SerializersModule = Json.serializersModule,
) : ChannelizedReportHandler {
    private val channelTypes: MutableList<Type> = mutableListOf()
    private val initialReports: MutableList<ChannelData<*>> = mutableListOf()

    private var writer: ParquetWriter<ChannelData<*>>? = null

    private val initialized: Boolean get() = writer != null

    override fun <T> initChannel(metadata: ChannelReport.ChannelMetadata<T>) {
        check(!initialized) {
            "Cannot initialize a channel on a report handler that has already been fully initialized"
        }
        val serializer = serializersModule.serializer(metadata.dataType)
        // Regardless of whether the original channel is nullable, the parquet column must be nullable to account for missing data.
        if (serializer.descriptor.isNullable) {
            System.err.println("Channel ${metadata.channel} is nullable. Since null is used to indicate lack of a report at that time, null reports will be indistinguishable from lack of a report.")
        }
        channelTypes.add(serializer.descriptor.nullable.toParquetMessageType(metadata.channel.toString()))

        TODO("Not yet implemented")
    }

    override fun <T> report(data: ChannelData<T>) {
        if (initialReports.isEmpty() || initialReports.first().time == data.time) {
            // This is (potentially) an initial report, so buffer it until time progresses
            initialReports.add(data)
            return
        }

        if (!initialized) {
            // This is the first certainly-not-initial report, so all channels are now initialized
            // Lock in our schema and build our writer
            writer = ChannelDataParquetWriterBuilder(path, channelTypes).build()
            // Flush all initial reports
            initialReports.forEach { writer!!.write(it) }
            initialReports.clear()
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
            PrimitiveKind.CHAR -> primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(1).`as`(LogicalTypeAnnotation.stringType())
            PrimitiveKind.BYTE -> primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(1).`as`(LogicalTypeAnnotation.intType(8))
            PrimitiveKind.SHORT -> primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(2).`as`(LogicalTypeAnnotation.intType(16))

            StructureKind.LIST -> Types.list(repetition)
                .element(getElementDescriptor(0).toParquetMessageType(getElementName(0)))

            StructureKind.MAP -> Types.map(repetition)
                .key(getElementDescriptor(0).toParquetMessageType(getElementName(0)))
                .value(getElementDescriptor(1).toParquetMessageType(getElementName(1)))

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

    private class ChannelDataParquetWriterBuilder(
        path: Path,
        private val channelTypes: List<Pair<Name, Type>>,
    ) : ParquetWriter.Builder<ChannelData<*>, ChannelDataParquetWriterBuilder>(
        LocalOutputFile(path)
    ) {
        private val channelInfo: Map<Name, Pair<Int, Type>> =
            // Add 1 to account for the timestamp field at index 0
            channelTypes.withIndex().associate { (index, pair) -> pair.first to (index + 1 to pair.second) }

        override fun self(): ChannelDataParquetWriterBuilder = this

        @Deprecated("Deprecated in Java")
        override fun getWriteSupport(conf: Configuration?): WriteSupport<ChannelData<*>?> =
            object : WriteSupport<ChannelData<*>?>() {
                private var recordConsumer: RecordConsumer? = null

                @Deprecated("Deprecated in Java")
                override fun init(configuration: Configuration?): WriteContext = WriteContext(
                    Types.buildMessage()
                        // TODO: Should we consider the possibility that the user didn't do their sim in UTC?
                        //   I think it's probably better to just assume they did, at least for now.
                        .addField(Types.required(PrimitiveTypeName.INT64)
                            .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                            .named("timestamp"))
                        .addFields(*channelTypes.map { it.second }.toTypedArray())
                        .named("root"),
                    mapOf(),
                )

                override fun prepareForWrite(recordConsumer: RecordConsumer?) {
                    this.recordConsumer = recordConsumer
                }

                override fun write(record: ChannelData<*>?) {
                    recordConsumer!!.apply {
                        startMessage()

                        startField("timestamp", 0)
                        // TODO: Do we need to handle overflows here?
                        addLong(record!!.time.epochSeconds * 1_000_000_000L + record.time.nanosecondsOfSecond)
                        endField("timestamp", 0)

                        val (index, type) = channelInfo.getValue(record.channel)
                        // Get the name from type to avoid re-computing channel.name.toString(), since string-building can be expensive.
                        startField(type.name, index)
                        // TODO: We need to run an appropriate serializer over the value here,
                        //   with a custom encoder that passes values on to the record consumer.
                        endField(type.name, index)

                        endMessage()
                    }
                }
            }
    }
}