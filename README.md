# Parakeet-Parquet

This is a library for [Parakeet](https://github.com/DavidLegg/Parakeet) providing direct-to-parquet file output.

[Parquet](https://parquet.apache.org/) is a binary columnar file format optimized for efficient analytics.
It is particularly well-suited to channelized time-series data, like that produced by a simulator.

## Usage

To use this, instantiate a `ParquetReportHandler` (or use the `Path.usingParquetReportHandler` extension method),
and pass the handler to `Simulator`'s constructor:

```kotlin
val myPlan: Plan
val parquet_path: Path

val simulator = parquet_path.usingParquetReportHandler { parquetReportHandler ->
    Simulator(
        reportHandler = parquetReportHandler,
        startTime = myPlan.startTime,
        constructModel = ::MyModel,
    )
}
simulator.runPlan(myPlan)
```

## Approach

This project combines Kotlin serializers with the Apache Parquet library to produce a Parakeet report handler.
The result is a low-copy system which uses very little CPU or memory.

### Kotlinx Serialization

A [Kotlin serializer](https://kotlinlang.org/docs/serialization.html) turns an in-memory object into a sequence of primitives that describe the data's shape and values.
These primitives take the form of method calls on an [Encoder](https://kotlinlang.org/api/kotlinx.serialization/kotlinx-serialization-core/kotlinx.serialization.encoding/-encoder/) like `beginStructure` and `encodeInt`.

Model authors are expected to write a serializer (or ask the compiler to do so for them using `@Serializable` annotations) for all types stored in cells or reported as simulation output.
They are required to do so regardless of output format, so Parakeet-Parquet doesn't impose additional development burden on model authors.
JSON lines, event CSV, and Parquet can all use the same serializers.

The Parakeet-Parquet library implements a custom `Encoder` accepting method calls from the model-provided serializers.

### Apache Parquet

The Apache Parquet library provides several ways to write a parquet file.
Among them is the `RecordConsumer` interface, which accepts a row in the parquet file and coordinates with other Apache Parquet library types
to buffer the data for that row until a complete row group is ready to write.
The library then transparently flushes this row group to disk, providing a row-oriented interface for streaming data to a parquet file.

The `RecordConsumer` asks us to describe a row through a series of method calls describing the data's shape and values, like `beginGroup` and `addInteger`.
This closely mirrors the expectations of a Kotlin encoder.
The Parakeet-Parquet `Encoder` realizes this correlation, translating Kotlin encoder calls into equivalent `RecordConsumer` calls.

### Parakeet Channelized Report Handler

When the simulator initializes, it describes all channels to this report handler, including type information.
This type information allows the report handler to look up appropriate Kotlin serializers.
Those serializers in turn describe the shape of each channel's data, letting us set a fixed schema for the parquet file at initialization.

## Limitations

The biggest limitation is that `Instant`s in the data are serialized as strings, rather than parquet timestamps.
This is a limitation of using Kotlin's serializers, which don't have special support for datetime types.
The timestamp for each report is serialized as a parquet timestamp, though this happens through special handling of that field.
There is currently no plan to remove this limitation.

We're also unable to take advantage of fixed-length parquet array types.
Once again, this is a limitation of the Kotlin serializer system, which supports records and variable-length collections, but not fixed-length collections.
This is a minor limitation, as any fixed-length collection could be represented as a variable-length collection for a small overhead in storage,
or as a record for a small inconvenience in schema.
Again, there is currently no plan to remove this limitation.

Finally, the options for tuning the resulting Parquet file are currently limited.
Settings like compression, row group sizes, and encoding options are currently fixed at their default values.
This provides reasonable performance in general, but making these configurable would be nice.
I am planning to remove this limitation in the future.
