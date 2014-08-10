/*
 * Copyright Vast 2012. All Rights Reserved.
 *
 * http://www.vast.com
 */
package athena.util;

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

    private long startTime = System.currentTimeMillis();

    /**
     * Constructs a new Rate.
     */
    public Rate() {
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
        buf.append(" ]");
        return buf.toString();
    }

    protected long getTime() {
        return System.currentTimeMillis() - startTime;
    }
}
