package gov.nasa.jpl.parakeet.arrow

import gov.nasa.jpl.parakeet.arrow.ArrowEncoder.State.*
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
    state: State,
) : Encoder, CompositeEncoder {
    constructor(writer: FieldWriter, serializersModule: SerializersModule) : this(writer, serializersModule, TopLevel)

    var position by writer::position

    private var state = state
        set(value) {
            debug { "S := $value (was $state)" }
            field = value
        }

    private val stack = ArrayDeque<Pair<FieldWriter, State>>(4)
    private fun save() {
        debug { "Save (W, $state)" }
        stack.addLast(writer to state)
    }
    private fun restore() {
        val (w, s) = stack.removeLast()
        debug { "Restore (W, $s)" }
        writer = w
        state = s
    }

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

    private val debug = true
    private fun debug(message: () -> String) {
        if (debug) println("DEBUG: " + "".padStart(stack.size * 2) + message())
    }

    @ExperimentalSerializationApi
    override fun encodeNull() {
        debug { "encodeNull() - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeNull()" }
                writer.writeNull()
            }
            WritingStructField -> { /* Nothing to do? */ }
            // TODO: We should probably be a little more careful about typing here...
            WritingListElement -> {
                debug { "W.writeNull()" }
                writer.writeNull()
            }
            // Null keys are not permitted
            WritingMapValue -> { /* Nothing to do */ }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeBoolean(value: Boolean) {
        debug { "encodeBoolean($value) - state: $state" }
        val bit = if (value) 1 else 0
        when (state) {
            TopLevel -> {
                debug { "W.writeBit($bit)" }
                writer.writeBit(bit)
            }
            WritingStructField -> {
                debug { "W.bit($fieldToWrite).writeBit($bit)" }
                writer.bit(fieldToWrite).writeBit(bit)
            }
            WritingListElement -> {
                debug { "W.bit().writeBit($bit)" }
                writer.bit().writeBit(bit)
            }
            WritingMapKey -> {
                debug { "W.key().bit().writeBit($bit)" }
                writer.key().bit().writeBit(bit)
            }
            WritingMapValue -> {
                debug { "W.value().bit().writeBit($bit)" }
                writer.value().bit().writeBit(bit)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeByte(value: Byte) {
        debug { "encodeByte($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeTinyInt($value)" }
                writer.writeTinyInt(value)
            }
            WritingStructField -> {
                debug { "W.tinyInt($fieldToWrite).writeTinyInt($value)" }
                writer.tinyInt(fieldToWrite).writeTinyInt(value)
            }
            WritingListElement -> {
                debug { "W.tinyInt().writeTinyInt($value)" }
                writer.tinyInt().writeTinyInt(value)
            }
            WritingMapKey -> {
                debug { "W.key().tinyInt().writeTinyInt($value)" }
                writer.key().tinyInt().writeTinyInt(value)
            }
            WritingMapValue -> {
                debug { "W.value().tinyInt().writeTinyInt($value)" }
                writer.value().tinyInt().writeTinyInt(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeShort(value: Short) {
        debug { "encodeShort($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeSmallInt($value)" }
                writer.writeSmallInt(value)
            }
            WritingStructField -> {
                debug { "W.smallInt($fieldToWrite).writeSmallInt($value)" }
                writer.smallInt(fieldToWrite).writeSmallInt(value)
            }
            WritingListElement -> {
                debug { "W.smallInt().writeSmallInt($value)" }
                writer.smallInt().writeSmallInt(value)
            }
            WritingMapKey -> {
                debug { "W.key().smallInt().writeSmallInt($value)" }
                writer.key().smallInt().writeSmallInt(value)
            }
            WritingMapValue -> {
                debug { "W.value().smallInt().writeSmallInt($value)" }
                writer.value().smallInt().writeSmallInt(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeChar(value: Char) {
        debug { "encodeChar($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeUInt2($value)" }
                writer.writeUInt2(value)
            }
            WritingStructField -> {
                debug { "W.uInt2($fieldToWrite).writeUInt2($value)" }
                writer.uInt2(fieldToWrite).writeUInt2(value)
            }
            WritingListElement -> {
                debug { "W.uInt2().writeUInt2($value)" }
                writer.uInt2().writeUInt2(value)
            }
            WritingMapKey -> {
                debug { "W.key().uInt2().writeUInt2($value)" }
                writer.key().uInt2().writeUInt2(value)
            }
            WritingMapValue -> {
                debug { "W.value().uInt2().writeUInt2($value)" }
                writer.value().uInt2().writeUInt2(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeInt(value: Int) {
        debug { "encodeInt($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeInt($value)" }
                writer.writeInt(value)
            }
            WritingStructField -> {
                debug { "W.integer($fieldToWrite).writeInt($value)" }
                writer.integer(fieldToWrite).writeInt(value)
            }
            WritingListElement -> {
                debug { "W.integer().writeInt($value)" }
                writer.integer().writeInt(value)
            }
            WritingMapKey -> {
                debug { "W.key().integer().writeInt($value)" }
                writer.key().integer().writeInt(value)
            }
            WritingMapValue -> {
                debug { "W.value().integer().writeInt($value)" }
                writer.value().integer().writeInt(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeLong(value: Long) {
        debug { "encodeLong($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeBigInt($value)" }
                writer.writeBigInt(value)
            }
            WritingStructField -> {
                debug { "W.bigInt($fieldToWrite).writeBigInt($value)" }
                writer.bigInt(fieldToWrite).writeBigInt(value)
            }
            WritingListElement -> {
                debug { "W.bigInt().writeBigInt($value)" }
                writer.bigInt().writeBigInt(value)
            }
            WritingMapKey -> {
                debug { "W.key().bigInt().writeBigInt($value)" }
                writer.key().bigInt().writeBigInt(value)
            }
            WritingMapValue -> {
                debug { "W.value().bigInt().writeBigInt($value)" }
                writer.value().bigInt().writeBigInt(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeFloat(value: Float) {
        debug { "encodeFloat($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeFloat4($value)" }
                writer.writeFloat4(value)
            }
            WritingStructField -> {
                debug { "W.float4($fieldToWrite).writeFloat4($value)" }
                writer.float4(fieldToWrite).writeFloat4(value)
            }
            WritingListElement -> {
                debug { "W.float4().writeFloat4($value)" }
                writer.float4().writeFloat4(value)
            }
            WritingMapKey -> {
                debug { "W.key().float4().writeFloat4($value)" }
                writer.key().float4().writeFloat4(value)
            }
            WritingMapValue -> {
                debug { "W.value().float4().writeFloat4($value)" }
                writer.value().float4().writeFloat4(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeDouble(value: Double) {
        debug { "encodeDouble($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeFloat8($value)" }
                writer.writeFloat8(value)
            }
            WritingStructField -> {
                debug { "W.float8($fieldToWrite).writeFloat8($value)" }
                writer.float8(fieldToWrite).writeFloat8(value)
            }
            WritingListElement -> {
                debug { "W.float8().writeFloat8($value)" }
                writer.float8().writeFloat8(value)
            }
            WritingMapKey -> {
                debug { "W.key().float8().writeFloat8($value)" }
                writer.key().float8().writeFloat8(value)
            }
            WritingMapValue -> {
                debug { "W.value().float8().writeFloat8($value)" }
                writer.value().float8().writeFloat8(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeString(value: String) {
        debug { "encodeString($value) - state: $state" }
        when (state) {
            TopLevel -> {
                debug { "W.writeVarChar($value)" }
                writer.writeVarChar(value)
            }
            WritingStructField -> {
                debug { "W.varChar($fieldToWrite).writeVarChar($value)" }
                writer.varChar(fieldToWrite).writeVarChar(value)
            }
            WritingListElement -> {
                debug { "W.varChar().writeVarChar($value)" }
                writer.varChar().writeVarChar(value)
            }
            WritingMapKey -> {
                debug { "W.key().varChar().writeVarChar($value)" }
                writer.key().varChar().writeVarChar(value)
            }
            WritingMapValue -> {
                debug { "W.value().varChar().writeVarChar($value)" }
                writer.value().varChar().writeVarChar(value)
            }
            else -> throw IllegalStateException("Encoder action not permitted in state $state")
        }
        endField()
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        debug { "encodeEnum(${enumDescriptor.getElementName(index)}) - state: $state" }
        encodeString(enumDescriptor.getElementName(index))
    }

    override fun encodeInline(descriptor: SerialDescriptor): Encoder {
        debug { "encodeInline(${descriptor.serialName}) - state: $state" }
        return this
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        debug { "beginStructure() - state: $state" }
        val structureKind = descriptor.kind as StructureKind
        save()
        when (structureKind) {
            StructureKind.CLASS -> {
                writer = when (state) {
                    TopLevel -> {
                        debug { "W := W (no-op)" }
                        writer
                    }
                    WritingStructField -> {
                        debug { "W := W.struct($fieldToWrite)" }
                        writer.struct(fieldToWrite)
                    }
                    WritingListElement -> {
                        debug { "W := W.struct()" }
                        writer.struct()
                    }
                    WritingMapKey -> {
                        debug { "W := W.key().struct()" }
                        writer.key().struct()
                    }
                    WritingMapValue -> {
                        debug { "W := W.value().struct()" }
                        writer.value().struct()
                    }
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
                debug { "W.start()" }
                writer.start()
                state = WritingStruct
            }
            StructureKind.LIST -> {
                writer = when (state) {
                    TopLevel -> {
                        debug { "W := W (no-op)" }
                        writer
                    }
                    WritingStructField -> {
                        debug { "W := W.list($fieldToWrite)" }
                        writer.list(fieldToWrite)
                    }
                    WritingListElement -> {
                        debug { "W := W.list()" }
                        writer.list()
                    }
                    WritingMapKey -> {
                        debug { "W := W.key().list()" }
                        writer.key().list()
                    }
                    WritingMapValue -> {
                        debug { "W := W.value().list()" }
                        writer.value().list()
                    }
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
                debug { "W.startList()" }
                writer.startList()
                state = WritingList
            }
            StructureKind.MAP -> {
                writer = when (state) {
                    TopLevel -> {
                        debug { "W := W (no-op)" }
                        writer
                    }
                    WritingStructField -> {
                        debug { "W := W.map($fieldToWrite)" }
                        writer.map(fieldToWrite)
                    }
                    WritingListElement -> {
                        debug { "W := W.map()" }
                        writer.map()
                    }
                    WritingMapKey -> {
                        debug { "W := W.key().map()" }
                        writer.key().map()
                    }
                    WritingMapValue -> {
                        debug { "W := W.value().map()" }
                        writer.value().map()
                    }
                    else -> throw IllegalStateException("Encoder action not permitted in state $state")
                } as FieldWriter
                debug { "W.startMap()" }
                writer.startMap()
                state = WritingMap
            }
            StructureKind.OBJECT -> throw AssertionError("Impossible code path")
        }
        // ASSUMPTION: The CompositeEncoder must be used immediately to encode the entire composite value before
        //   "this" encoder can be used to encode any other part of the parent structure.
        //   With this assumption, it's safe to re-use this encoder instance, with the state to restore to saved in a stack.
        return this
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        debug { "endStructure() - state: $state" }
        when (descriptor.kind as StructureKind) {
            StructureKind.CLASS -> {
                debug { "W.end()" }
                writer.end()
            }
            StructureKind.LIST -> {
                debug { "W.endList()" }
                writer.endList()
            }
            StructureKind.MAP -> {
                debug { "W.endMap()" }
                writer.endMap()
            }
            StructureKind.OBJECT -> {
                throw AssertionError("Impossible code path")
            }
        }
        restore()
        endField()
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
                debug { "W.startEntry()" }
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
                debug { "W.endEntry()" }
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
        debug { "encodeBooleanElement($value) - state: $state" }
        startField(descriptor, index)
        encodeBoolean(value)
    }

    override fun encodeByteElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Byte
    ) {
        debug { "encodeByteElement($value) - state: $state" }
        startField(descriptor, index)
        encodeByte(value)
    }

    override fun encodeShortElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Short
    ) {
        debug { "encodeShortElement($value) - state: $state" }
        startField(descriptor, index)
        encodeShort(value)
    }

    override fun encodeCharElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Char
    ) {
        debug { "encodeCharElement($value) - state: $state" }
        startField(descriptor, index)
        encodeChar(value)
    }

    override fun encodeIntElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Int
    ) {
        debug { "encodeIntElement($value) - state: $state" }
        startField(descriptor, index)
        encodeInt(value)
    }

    override fun encodeLongElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Long
    ) {
        debug { "encodeLongElement($value) - state: $state" }
        startField(descriptor, index)
        encodeLong(value)
    }

    override fun encodeFloatElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Float
    ) {
        debug { "encodeFloatElement($value) - state: $state" }
        startField(descriptor, index)
        encodeFloat(value)
    }

    override fun encodeDoubleElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: Double
    ) {
        debug { "encodeDoubleElement($value) - state: $state" }
        startField(descriptor, index)
        encodeDouble(value)
    }

    override fun encodeStringElement(
        descriptor: SerialDescriptor,
        index: Int,
        value: String
    ) {
        debug { "encodeStringElement($value) - state: $state" }
        startField(descriptor, index)
        encodeString(value)
    }

    override fun encodeInlineElement(
        descriptor: SerialDescriptor,
        index: Int
    ): Encoder {
        debug { "encodeInlineElement() - state: $state" }
        startField(descriptor, index)
        return this
    }

    override fun <T> encodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T
    ) {
        debug { "encodeSerializableElement($value) - state: $state" }
        startField(descriptor, index)
        serializer.serialize(this, value)
    }

    @ExperimentalSerializationApi
    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?
    ) {
        debug { "encodeNullableSerializableElement($value) - state: $state" }
        if (value == null) {
            startField(descriptor, index)
            encodeNull()
        } else {
            encodeSerializableElement(descriptor, index, serializer, value)
        }
    }
}