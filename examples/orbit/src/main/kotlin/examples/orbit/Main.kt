package examples.orbit

import gov.nasa.jpl.parakeet.examples.orbit.EarthOrbit
import gov.nasa.jpl.parakeet.foundation.Simulator
import gov.nasa.jpl.parakeet.parquet.ParquetReportHandler
import kotlin.io.path.Path
import kotlin.io.path.absolute
import kotlin.time.Duration.Companion.days
import kotlin.time.Instant
import kotlin.time.times

fun main(vararg args: String) {
    val startTime = Instant.parse("2000-01-01T00:00:00Z")
    val outputPath = Path(args[0]).absolute()
    val numberOfYears = args[1].toInt()
    val endTime = startTime + (numberOfYears * 365.25.days)

    println("Simulating for $numberOfYears years from $startTime to $endTime")
    val realTimeStart = System.currentTimeMillis()

    ParquetReportHandler(outputPath).use { parquetReportHandler ->
        Simulator(
            reportHandler = parquetReportHandler,
            startTime = startTime,
            constructModel = ::EarthOrbit
        ).runUntil(endTime)
    }

    val realTimeEnd = System.currentTimeMillis()
    val realDurationSeconds = (realTimeEnd - realTimeStart) / 1000.0
    println("Done in $realDurationSeconds s")
}