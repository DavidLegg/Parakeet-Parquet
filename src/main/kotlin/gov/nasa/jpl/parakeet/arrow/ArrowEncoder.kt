package gov.nasa.jpl.parakeet.arrow

import gov.nasa.jpl.parakeet.arrow.ArrowEncoder.State.*
import gov.nasa.jpl.parakeet.general.units.Field
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.arrow.vector.complex.writer.FieldWriter

/**
 * An [Encoder] which connects to an Arrow vector writer.
 */
class ArrowEncoder private constructor(
    private var writer: FieldWriter,
    override val serializersModule: SerializersModule,
    private var state: State,
) : Encoder, CompositeEncoder {
    constructor(writer: FieldWriter, serializersModule: SerializersModule) : this(writer, serializersModule, TopLevel)

    var position by writer::position

    private enum class State {
        TopLevel,
        WritingStruct,
        WritingStructField,
        WritingList,
        WritingListElement,
        WritingMap,
        WritingMapKey,
        WritingMapValue,
        Done
    }

    private var fieldToWrite: String? = null

    @ExperimentalSerializationApi
    override fun encodeNull() {
        when (state) {
            TopLevel -> writer.writeNull()
            WritingStructField -> { /* Nothing to do? */ }
            // TODO: We should probably be a little more careful about typing here...
            WritingListElement -> writer.writeNull()
            // Null keys are not permitted
            WritingMapValue -> { /* Nothing to do */ }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeBoolean(value: Boolean) {
        val bit = if (value) 1 else 0
        when (state) {
            TopLevel -> writer.writeBit(bit)
            WritingStructField -> writer.bit(fieldToWrite).writeBit(bit)
            WritingListElement -> writer.bit().writeBit(bit)
            WritingMapKey -> writer.key().bit().writeBit(bit)
            WritingMapValue -> writer.value().bit().writeBit(bit)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeByte(value: Byte) {
        when (state) {
            TopLevel -> writer.writeTinyInt(value)
            WritingStructField -> writer.tinyInt(fieldToWrite).writeTinyInt(value)
            WritingListElement -> writer.tinyInt().writeTinyInt(value)
            WritingMapKey -> writer.key().tinyInt().writeTinyInt(value)
            WritingMapValue -> writer.value().tinyInt().writeTinyInt(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeShort(value: Short) {
        when (state) {
            TopLevel -> writer.writeSmallInt(value)
            WritingStructField -> writer.smallInt(fieldToWrite).writeSmallInt(value)
            WritingListElement -> writer.smallInt().writeSmallInt(value)
            WritingMapKey -> writer.key().smallInt().writeSmallInt(value)
            WritingMapValue -> writer.value().smallInt().writeSmallInt(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeChar(value: Char) {
        when (state) {
            TopLevel -> writer.writeUInt2(value)
            WritingStructField -> writer.uInt2(fieldToWrite).writeUInt2(value)
            WritingListElement -> writer.uInt2().writeUInt2(value)
            WritingMapKey -> writer.key().uInt2().writeUInt2(value)
            WritingMapValue -> writer.value().uInt2().writeUInt2(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeInt(value: Int) {
        when (state) {
            TopLevel -> writer.writeInt(value)
            WritingStructField -> writer.integer(fieldToWrite).writeInt(value)
            WritingListElement -> writer.integer().writeInt(value)
            WritingMapKey -> writer.key().integer().writeInt(value)
            WritingMapValue -> writer.value().integer().writeInt(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeLong(value: Long) {
        when (state) {
            TopLevel -> writer.writeBigInt(value)
            WritingStructField -> writer.bigInt(fieldToWrite).writeBigInt(value)
            WritingListElement -> writer.bigInt().writeBigInt(value)
            WritingMapKey -> writer.key().bigInt().writeBigInt(value)
            WritingMapValue -> writer.value().bigInt().writeBigInt(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeFloat(value: Float) {
        when (state) {
            TopLevel -> writer.writeFloat4(value)
            WritingStructField -> writer.float4(fieldToWrite).writeFloat4(value)
            WritingListElement -> writer.float4().writeFloat4(value)
            WritingMapKey -> writer.key().float4().writeFloat4(value)
            WritingMapValue -> writer.value().float4().writeFloat4(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeDouble(value: Double) {
        when (state) {
            TopLevel -> writer.writeFloat8(value)
            WritingStructField -> writer.float8(fieldToWrite).writeFloat8(value)
            WritingListElement -> writer.float8().writeFloat8(value)
            WritingMapKey -> writer.key().float8().writeFloat8(value)
            WritingMapValue -> writer.value().float8().writeFloat8(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeString(value: String) {
        when (state) {
            TopLevel -> writer.writeVarChar(value)
            WritingStructField -> writer.varChar(fieldToWrite).writeVarChar(value)
            WritingListElement -> writer.varChar().writeVarChar(value)
            WritingMapKey -> writer.key().varChar().writeVarChar(value)
            WritingMapValue -> writer.value().varChar().writeVarChar(value)
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        encodeString(enumDescriptor.getElementName(index))
    }

    override fun encodeInline(descriptor: SerialDescriptor): Encoder {
        return this
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        val structureKind = descriptor.kind as StructureKind
        val innerEncoderState: State
        val innerWriter: FieldWriter
        when (structureKind) {
            StructureKind.CLASS -> {
                innerEncoderState = WritingStruct
                innerWriter = when (state) {
                    TopLevel -> writer
                    WritingStructField -> writer.struct(fieldToWrite)
                    WritingListElement -> writer.struct()
                    WritingMapKey -> writer.key().struct()
                    WritingMapValue -> writer.value().struct()
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
            }
            StructureKind.LIST -> {
                innerEncoderState = WritingList
                innerWriter = when (state) {
                    TopLevel -> writer
                    WritingStructField -> writer.list(fieldToWrite)
                    WritingListElement -> writer.list()
                    WritingMapKey -> writer.key().list()
                    WritingMapValue -> writer.value().list()
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
                innerWriter.startList()
            }
            StructureKind.MAP -> {
                innerEncoderState = WritingMap
                innerWriter = when (state) {
                    TopLevel -> writer
                    WritingStructField -> writer.map(fieldToWrite)
                    WritingListElement -> writer.map()
                    WritingMapKey -> writer.key().map()
                    WritingMapValue -> writer.value().map()
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
                innerWriter.startMap()
            }
            StructureKind.OBJECT -> throw AssertionError("Impossible code path")
        }
        state = when (state) {
            TopLevel -> TopLevel
            WritingListElement -> WritingList
            WritingStructField -> WritingStruct
            WritingMapKey, WritingMapValue -> WritingMap
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        // TODO: Should we re-use this object, with some way to restore back to current state once the Composite is done?
        // Would that be more efficient, avoiding the creation of this temporary encoder?
        // If not, could we hang on to this encoder somehow to re-use it?
        return ArrowEncoder(innerWriter, serializersModule, innerEncoderState)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        when (descriptor.kind as StructureKind) {
            StructureKind.CLASS -> { /* Nothing to do for CLASS */ }
            StructureKind.LIST -> writer.endList()
            StructureKind.MAP -> writer.endMap()
            StructureKind.OBJECT -> throw AssertionError("Impossible code path")
        }
        state = Done
    }

    // Universal helper for CompositeEncoder so we can re-use Encoder logic
    // Decomposes CompositeEncoder.write___Element into startField + Encoder.write___
    private fun startField(descriptor: SerialDescriptor, index: Int) {
        state = when (state) {
            WritingStruct -> {
                fieldToWrite = descriptor.getElementName(index)
                WritingStructField
            }
            WritingList -> WritingListElement
            WritingMap -> if (index % 2 == 0) {
                writer.startEntry()
                WritingMapKey
            } else {
                WritingMapValue
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
    }

    private fun endField() {
        state = when (state) {
            // Asymmetry: We can end a field at the top level which we never started.
            //   This is more a quirk in how I organized this code than a logical way to do things,
            //   but it's simpler to just allow it than to rewrite the code "cleanly" with extra states or extra logic.
            TopLevel -> TopLevel
            WritingStructField -> WritingStruct
            WritingListElement -> WritingList
            WritingMapKey -> WritingMap
            WritingMapValue -> {
                writer.endEntry()
                WritingMap
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
    }

    override fun encodeBooleanElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Boolean
    ) {
        startField(descriptor, index)
        encodeBoolean(value)
    }

    override fun encodeByteElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Byte
    ) {
        startField(descriptor, index)
        encodeByte(value)
    }

    override fun encodeShortElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Short
    ) {
        startField(descriptor, index)
        encodeShort(value)
    }

    override fun encodeCharElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Char
    ) {
        startField(descriptor, index)
        encodeChar(value)
    }

    override fun encodeIntElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Int
    ) {
        startField(descriptor, index)
        encodeInt(value)
    }

    override fun encodeLongElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Long
    ) {
        startField(descriptor, index)
        encodeLong(value)
    }

    override fun encodeFloatElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Float
    ) {
        startField(descriptor, index)
        encodeFloat(value)
    }

    override fun encodeDoubleElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Double
    ) {
        startField(descriptor, index)
        encodeDouble(value)
    }

    override fun encodeStringElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: String
    ) {
        startField(descriptor, index)
        encodeString(value)
    }

    override fun encodeInlineElement(
        descriptor: SerialDescriptor,
        index: Int
    ): Encoder {
        startField(descriptor, index)
        return this
    }

    override fun <T> encodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T
    ) {
        startField(descriptor, index)
        // Preserve the current writer so we can restore it after writing this element
        val startingWriter = writer
        val elementDescriptor = descriptor.getElementDescriptor(index)
        // Configure this object to write the inner element
        writer = when (elementDescriptor.kind) {
            // When the inner element is a primitive, it's like an inline call. No additional configuration needed.
            is PrimitiveKind, SerialKind.ENUM -> writer
            is StructureKind.CLASS -> when (state) {
                WritingListElement -> writer.struct()
                WritingStructField -> writer.struct(fieldToWrite)
                WritingMapKey -> writer.key().struct()
                WritingMapValue -> writer.value().struct()
                else -> throw IllegalStateException("Encoder action not permitted in state $state")
            }
            is StructureKind.LIST -> when (state) {
                WritingListElement -> writer.list()
                WritingStructField -> writer.list(fieldToWrite)
                WritingMapKey -> writer.key().list()
                WritingMapValue -> writer.value().list()
                else -> throw IllegalStateException("Encoder action not permitted in state $state")
            }
            is StructureKind.MAP -> when (state) {
                WritingListElement -> writer.map()
                WritingStructField -> writer.map(fieldToWrite)
                WritingMapKey -> writer.key().map()
                WritingMapValue -> writer.value().map()
                else -> throw IllegalStateException("Encoder action not permitted in state $state")
            }
            else -> throw AssertionError("Impossible code path")
        } as FieldWriter
        // Write the indicated serializable element
        serializer.serialize(this, value)
        // And finally restore this object to continue writing the outer composite
        writer = startingWriter
    }

    @ExperimentalSerializationApi
    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?
    ) {
        if (value == null) {
            startField(descriptor, index)
            encodeNull()
        } else {
            encodeSerializableElement(descriptor, index, serializer, value)
        }
    }
}