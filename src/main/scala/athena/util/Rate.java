/*
 * Copyright Vast 2012. All Rights Reserved.
 *
 * http://www.vast.com
 */
package athena.util;

import org.apache.commons.lang.time.StopWatch;

/**
 * This is a copy of the Rate class from Vast's common-lang-utils library.
 * It has been inlined here to avoid Vast dependencies in the Athena library.
 *
 * @author Jason Duke (jduke@vast.com)
 */
public class Rate {

    private static final long MILLIS_PER_SECOND = 1000;
    private static final long MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60;
    private static final long MILLIS_PER_HOUR = MILLIS_PER_MINUTE * 60;
    private static final long SECS_PER_MINUTE = 60;
    private static final long SECS_PER_HOUR = SECS_PER_MINUTE * 60;

    private StopWatch sw;
    private long actualOperationCount = 0;
    private Long expectedOperationCount = null;

    /**
     * Constructs a new Rate.
     */
    public Rate() {
        this(null);
    }

    /**
     * Constructs a new Rate with the given value for the expectedOperationCount.
     *
     * @param expectedOperationCount the expected number of operations.
     */
    public Rate(Long expectedOperationCount) {
        this.expectedOperationCount = expectedOperationCount;
        this.sw = new StopWatch();
        this.sw.start();
    }

    /**
     * Adjusts the operation count by the given delta value.
     *
     * @param delta the given delta value.
     */
    public void adjustOperationCount(long delta) {
        actualOperationCount += delta;
        if (actualOperationCount < 0) {
            actualOperationCount = 0;
        }
    }

    /**
     * Returns the actual operation count.
     *
     * @return the actual operation count
     */
    public long getActualOperationCount() {
        return actualOperationCount;
    }

    /**
     * Returns the expected operation count.
     *
     * @return the expected operation count
     */
    public Long getExpectedOperationCount() {
        return expectedOperationCount;
    }

    /**
     * Resets the Rate object by restarting the stopwatch and setting the actualOperationCount back to zero.
     */
    public void reset() {
        sw.reset();
        sw.start();
        actualOperationCount = 0;
    }

    /**
     * Resets the Rate object by restarting the stopwatch, setting the actualOperationCount back to zero, and setting
     * the expectedOperationCount to the given value.
     *
     * @param expectedOperationCount
     */
    public void reset(Long expectedOperationCount) {
        reset();
        this.expectedOperationCount = expectedOperationCount;
    }

    /**
     * Sets the expected operation count to the given value.
     *
     * @param expectedOperationCount the expected operation count
     */
    public void setExpectedOperationCount(Long expectedOperationCount) {
        this.expectedOperationCount = expectedOperationCount;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        long millis = getTime();

        long msecs = millis;
        long hours = msecs / MILLIS_PER_HOUR;
        msecs = msecs % MILLIS_PER_HOUR;
        long minutes = msecs / MILLIS_PER_MINUTE;
        msecs = msecs % MILLIS_PER_MINUTE;
        long seconds = msecs / MILLIS_PER_SECOND;
        msecs = msecs % MILLIS_PER_SECOND;

        StringBuilder buf = new StringBuilder();
        buf.append("[ ");
        buf.append(String.format("Elapsed: %02d:%02d:%02d.%03d", new Object[]{hours, minutes, seconds, msecs}));
        if (actualOperationCount > 0 || (expectedOperationCount != null && expectedOperationCount > 0)) {
            buf.append(String.format("  Count: %d", actualOperationCount));
            if (expectedOperationCount != null) {
                buf.append(String.format(" of %d", expectedOperationCount));
            }
            if (actualOperationCount > 0 && millis > 0) {
                double ratePerSecond = millis == 0 ? 0 : actualOperationCount / (millis * 1.0) * 1000;
                if (ratePerSecond > 1) {
                    buf.append(String.format(" (%f/sec)", ratePerSecond));
                } else {
                    double ratePerMinute = ratePerSecond * SECS_PER_MINUTE;
                    if (ratePerMinute > 1) {
                        buf.append(String.format(" (%f/min)", ratePerMinute));
                    } else {
                        double ratePerHour = ratePerSecond * SECS_PER_HOUR;
                        buf.append(String.format(" (%f/hr)", ratePerHour));
                    }
                }
                if (expectedOperationCount != null) {
                    double etaTotalSecs = (expectedOperationCount - actualOperationCount) / ratePerSecond;
                    long etaSecs = (long) etaTotalSecs;
                    long etaHours = etaSecs / SECS_PER_HOUR;
                    etaSecs = etaSecs % SECS_PER_HOUR;
                    long etaMinutes = etaSecs / SECS_PER_MINUTE;
                    etaSecs = etaSecs % -SECS_PER_MINUTE;
                    buf.append(String.format("  Remaining: %02d:%02d:%02d", new Object[]{etaHours, etaMinutes, etaSecs}));
                }
            }
        }
        buf.append(" ]");
        return buf.toString();
    }

    protected long getTime() {
        return sw.getTime();
    }
}
