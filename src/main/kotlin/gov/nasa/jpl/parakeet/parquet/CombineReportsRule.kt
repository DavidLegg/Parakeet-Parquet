package gov.nasa.jpl.parakeet.parquet

/**
 * The [ParquetReportHandler] can write multiple reports at the same time in one row, provided they are all on different channels.
 * When it does this, it needs a policy for how to handle multiple reports at the same time on one channel.
 *
 * This enum spells out the options for such a policy, as well as a [DONT_COMBINE] option to write one report per row.
 */
enum class CombineReportsRule {
    /**
     * Do not combine reports.
     * Every report gets its own row, which exactly preserves report order from the simulator.
     */
    DONT_COMBINE,

    /**
     * Combine reports across different channels while preserving all reports.
     * If a channel produces multiple reports at the same time, a new row is started.
     */
    COMBINE_AND_KEEP_ALL,

    /**
     * Combine reports across different channels, keeping only the last report for each channel at each time.
     * Guarantees at most one row per time.
     */
    COMBINE_AND_KEEP_LAST
}