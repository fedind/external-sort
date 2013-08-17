package utils.externalsort;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author fedin
 */
public class Sorter {

    public static void sort(BlockingQueue<File> queue, String fileName) {
        File inputFile = new File(fileName);
        Utils.threadPrint(String.format("Sorter.start: fileName = %s, size = %dB\n", inputFile, inputFile.length()));
        int bytesProcessed = 0;
        FileInputStream is = null;
        try {
            is = new FileInputStream(inputFile);
            FileChannel in = is.getChannel();
            IntBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size()).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
            Utils.memory();
            final int bufferSize = 5000000 / Utils.BYTE_TO_INT;
            Utils.threadPrint(String.format("Sorter.bufferSize = %d\n", bufferSize));
            int[] arr = new int[bufferSize];
            Utils.memory();
            int remaining = 0;
            while ((remaining = buf.remaining()) > 0) {
                final int sz = remaining < bufferSize ? remaining : bufferSize;
                bytesProcessed += sz;
                buf.get(arr, 0, sz);
                Arrays.sort(arr, 0, sz);
                queue.put(Utils.saveTemp(arr, 0, sz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            Utils.close(is);
            Utils.threadPrint(String.format("Sorter.finish: bytesProcessed = %dB\n", bytesProcessed * Utils.BYTE_TO_INT));
        }
    }

    protected int calcBufferSize() {
        return (int) (Runtime.getRuntime().freeMemory() - 1024 * 1024 * 2) / Utils.BYTE_TO_INT;
    }
}
