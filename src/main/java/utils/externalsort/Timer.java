package utils.externalsort;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author fedin
 */
public class Timer {

    public static Timer start() {
        return new Timer().reset();
    }

    private long start;

    private Timer() {
    }


    public Timer reset() {
        start = System.nanoTime();
        return this;
    }

    @Override
    public String toString() {
        return String.format("elapsed time: %,dms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.start));
    }
}
