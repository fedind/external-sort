package utils.externalsort;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author fedin
 */
public class Sorter implements Runnable {

    private static final int CHUNK = 5000000;
    private static final int CHUNK_INT = CHUNK / Utils.BYTE_TO_INT;

    public static Sorter create(BlockingQueue<File> queue, String fileName, AtomicBoolean stop) {
        final Sorter sorter = new Sorter();
        sorter.queue = Objects.requireNonNull(queue);
        sorter.stop = Objects.requireNonNull(stop);
        sorter.fileName = fileName;
        return sorter;
    }

    public static File saveTemp(int[] arr, int offset, int length) throws IOException {
        DataOutputStream out = null;
        final File tempFile = File.createTempFile(Utils.TEMP_FILE_PREFIX, null);
        tempFile.deleteOnExit();
        try {
            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)));
            for (int i = offset; i < length; i++) {
                out.writeInt(arr[i]);
            }
        } finally {
            Utils.close(out);
            System.out.println(tempFile);
        }
        return tempFile;
    }
    private BlockingQueue<File> queue;
    private String fileName;
    private AtomicBoolean stop;

    private Sorter() {
    }

    public void run() {
        FileInputStream is = null;
        try {
            is = new FileInputStream(fileName);
            FileChannel in = is.getChannel();
            IntBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size()).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
            System.out.println("in.size() = " + in.size());
            Utils.memory();
            final int bufferSize = 5000000 / Utils.BYTE_TO_INT;
            System.out.println("Sorter: bufferSize = " + bufferSize * Utils.BYTE_TO_INT);
            int[] arr = new int[bufferSize];
            Utils.memory();
            int remaining = 0;
            while ((remaining = buf.remaining()) > 0) {
                final int sz = remaining < bufferSize ? remaining : bufferSize;
                buf.get(arr, 0, sz);
                Arrays.sort(arr, 0, sz);
                queue.put(saveTemp(arr, 0, sz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            Utils.close(is);
            stop.set(true);
        }
    }

    protected int calcBufferSize() {
        return (int) (Runtime.getRuntime().freeMemory() - 1024 * 1024 * 2) / Utils.BYTE_TO_INT;
    }
}
