package utils.externalsort;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author fedin
 */
public class Utils {

    public static final int BYTE_TO_INT = Integer.SIZE / Byte.SIZE;
    public static final String TEMP_FILE_PREFIX = "external-sort";

    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static boolean and(boolean[] arr) {
        boolean res = true;
        for (boolean b : arr) {
            res = b && res;
        }
        return res;
    }

    public static void memory() {
        System.out.println(Thread.currentThread());        
        final long free = Runtime.getRuntime().freeMemory();
        final long total = Runtime.getRuntime().totalMemory();
        System.out.format("free memory: %,dKB\n", free / (1024));
        System.out.format("max memory: %,dKB\n", Runtime.getRuntime().maxMemory() / (1024));
        System.out.format("total memory: %,dKB\n", total / (1024));
        System.out.format("total - free : %,dKB\n", (total - free) / (1024));

    }
}
