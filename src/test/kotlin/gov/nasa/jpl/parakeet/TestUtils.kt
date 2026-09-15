package gov.nasa.jpl.parakeet.parquet

import org.jetbrains.kotlinx.dataframe.DataFrame
import org.junit.jupiter.api.Assertions.assertEquals

object TestUtils {
    operator fun <T> List<T>.component6(): T = get(5)
    operator fun <T> List<T>.component7(): T = get(6)
    operator fun <T> List<T>.component8(): T = get(7)
    operator fun <T> List<T>.component9(): T = get(8)
    operator fun <T> List<T>.component10(): T = get(9)

    fun checkDataFrame(dataframe: DataFrame<*>, vararg expectedColumns: String, block: DataFrameChecker.() -> Unit) {
        assertEquals(expectedColumns.toList(), dataframe.columnNames()) {
            "DataFrame was expected to have columns ${expectedColumns.toList()} but actually had columns ${dataframe.columnNames()}"
        }

        val numRows = dataframe.rowsCount()
        val numCols = dataframe.columnsCount()

        var rowIndex = 0
        val checker = object : DataFrameChecker {
            override fun row(block: DataRowChecker.() -> Unit) {
                if (rowIndex < numRows) {
                    // Run this check
                    val row = dataframe[rowIndex]
                    var colIndex = 0
                    val rowChecker = object : DataRowChecker {
                        override fun check(block: (Any?) -> Unit) {
                            if (colIndex < numCols) {
                                val element = row[colIndex]
                                block(element)
                            }
                            // Similar to rows, continue to increment column index even if we've run out of columns
                            // When we check column index below, if we got the number of columns wrong,
                            // this will give an accurate error message.
                            colIndex++
                        }
                    }
                    rowChecker.block()

                    assertEquals(expectedColumns.size, colIndex) {
                        "Invalid test setup: expected ${expectedColumns.size} columns but $colIndex were specified"
                    }
                }
                // We can't run the check since there's no data.
                // Don't fail immediately, though. Instead, continue to increment rowIndex.
                // The check of rowIndex at the end will catch the mismatch in the number of rows and fail the test,
                // but it will do so with an accurate expectation for the number of rows.
                rowIndex++
            }
        }
        checker.block()

        assertEquals(rowIndex, dataframe.rowsCount()) {
            "DataFrame was expected to have $rowIndex rows but actually had ${dataframe.rowsCount()} rows"
        }
    }

    interface DataFrameChecker {
        fun row(block: DataRowChecker.() -> Unit)
    }

    interface DataRowChecker {
        fun check(block: (Any?) -> Unit)
    }

    fun DataRowChecker.assertEquals(expectedValue: Any?) {
        check { actualValue ->
            if (expectedValue != ANYTHING) assertEquals(expectedValue, actualValue)
        }
    }

    fun DataFrameChecker.rowEquals(vararg expectedValues: Any?) {
        row {
            for (expectedValue in expectedValues) {
                assertEquals(expectedValue)
            }
        }
    }

    /**
     * Sentinel value used by [assertEquals] and [rowEquals] to indicate that any actual value is acceptable.
     */
    object ANYTHING
}