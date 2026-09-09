package examples.orbit

import gov.nasa.jpl.parakeet.examples.orbit.EarthOrbit
import gov.nasa.jpl.parakeet.foundation.Simulator
import gov.nasa.jpl.parakeet.foundation.reporting.ChannelizedReportHandler
import gov.nasa.jpl.parakeet.general.reporting.ParallelReportHandler.Companion.inParallel
import gov.nasa.jpl.parakeet.general.reporting.ReportHandling.jsonlReportHandler
import gov.nasa.jpl.parakeet.general.reporting.usingEventCsvReportHandler
import gov.nasa.jpl.parakeet.parquet.CombineReportsRule
import gov.nasa.jpl.parakeet.parquet.usingParquetReportHandler
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.absolute
import kotlin.io.path.deleteIfExists
import kotlin.io.path.deleteRecursively
import kotlin.io.path.div
import kotlin.io.path.fileSize
import kotlin.io.path.name
import kotlin.io.path.outputStream
import kotlin.time.Duration.Companion.days
import kotlin.time.DurationUnit.SECONDS
import kotlin.time.Instant
import kotlin.time.measureTime
import kotlin.time.times

@OptIn(ExperimentalPathApi::class)
fun main(vararg args: String) {
    val args = args.toMutableList()

    if (args.remove("--grid")) {
        val outputDir = Path(args[0]).toAbsolutePath()
        val reps = if (args.size > 1) args[1].toInt() else 10

        outputDir.deleteRecursively()
        outputDir.toFile().mkdirs()
        gridTest(outputDir, reps = reps)
    } else {
        val parallel = args.remove("--parallel")
        val outputPath = Path(args[0]).absolute()
        val numYears = args[1].toInt()

        val duration = measureTime {
            runSimulation(outputPath, numYears, parallel)
        }
        println("Done. Runtime (s): ${duration.toDouble(SECONDS)}")
    }
}

fun gridTest(outputDir: Path, reps: Int) {
    val yearOptions = listOf(100,200,500,1000,2000,5000,10_000)
    val yearLength = yearOptions.maxOf { it.toString().length }
    val parallelOptions = listOf(false, true)
    val threadingLength = maxOf("serial".length, "parallel".length)
    val formatOptions = listOf(/*"jsonl", */"csv", "parquet", "condensed.parquet")
    val formatLength = formatOptions.maxOf { it.length }
    val totalReps = yearOptions.size * reps * parallelOptions.size * formatOptions.size
    val repsLength = totalReps.toString().length
    var repCount = 0
    val runtimeCsv = outputDir / "runtime.csv"
    runtimeCsv.outputStream().use {
        it.writer().use { runtimeCsvWriter ->
            runtimeCsvWriter.write("Plan Length (yr),Threading,Format,Runtime (s)\n")
            for (numYears in yearOptions) {
                for (i in 1..reps) {
                    for (parallel in parallelOptions) {
                        val threading = if (parallel) "parallel" else "serial"
                        for (format in formatOptions) {
                            ++repCount
                            print("Running simulation (%${repsLength}d/%d): %${yearLength}d years / %${threadingLength}s / %${formatLength}s".format(
                                repCount,
                                totalReps,
                                numYears,
                                threading,
                                format,
                            ))
                            System.out.flush()

                            val outputPath = outputDir / "orbit-$numYears-year.$threading.$format"
                            outputPath.deleteIfExists()
                            val duration = measureTime {
                                runSimulation(outputPath, numYears, parallel)
                            }
                            runtimeCsvWriter.write("$numYears,$threading,$format,${duration.toDouble(SECONDS)}\n")
                            runtimeCsvWriter.flush()

                            println(" [%5.1f s, %6.1f MB]".format(duration.toDouble(SECONDS), outputPath.fileSize() / (1024.0 * 1024.0)));
                        }
                    }
                }
            }
        }
    }
    println("Grid test complete. Runtime data saved to ${runtimeCsv.toAbsolutePath()}")
}

fun runSimulation(outputPath: Path, numYears: Int, parallel: Boolean) {
    fun runSimulation(reportHandler: ChannelizedReportHandler) {
        runSimulation(reportHandler, numYears, parallel)
    }

    outputPath.dispatchOnEnding(
        ".condensed.parquet" to {
            outputPath.usingParquetReportHandler(
                EarthOrbit.JSON_FORMAT.serializersModule,
                combineReportsRule = CombineReportsRule.COMBINE_AND_KEEP_LAST,
                block = ::runSimulation,
            )
        },
        ".parquet" to {
            outputPath.usingParquetReportHandler(
                EarthOrbit.JSON_FORMAT.serializersModule,
                block = ::runSimulation,
            )
        },
        ".csv" to {
            outputPath.toFile().usingEventCsvReportHandler(
                EarthOrbit.JSON_FORMAT,
                block = ::runSimulation,
            )
        },
        ".jsonl" to {
            outputPath.outputStream().use {
                runSimulation(jsonlReportHandler(it, EarthOrbit.JSON_FORMAT))
            }
        },
    )
}

fun <R> Path.dispatchOnEnding(vararg actions: Pair<String, () -> R>): R {
    for ((ending, action) in actions) {
        if (name.endsWith(ending)) {
            return action()
        }
    }
    throw IllegalArgumentException("No action configured for file name: $name")
}

fun runSimulation(reportHandler: ChannelizedReportHandler, numYears: Int, parallel: Boolean = false) {
    if (parallel) {
        reportHandler.inParallel { runSimulation(it, numYears) }
    } else {
        val startTime = Instant.parse("2000-01-01T00:00:00Z")
        Simulator(
            reportHandler = reportHandler,
            startTime = startTime,
            constructModel = ::EarthOrbit
        ).runUntil(startTime + (numYears * 365.25.days))
    }
}
