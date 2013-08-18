package utils.externalsort;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    public static void parallelSort(final int[] arr, final int fromIndex, final int toIndex, final int threadsNumber) {
        final int len = toIndex - fromIndex;
        final int chunkSize = len / threadsNumber;
        ExecutorService ex = Executors.newFixedThreadPool(threadsNumber);
        class Chunk {

            public Chunk(Chunk next) {
                this.next = next;
            }
            Chunk next;
        }
        final List<Future<Chunk>> futures = new ArrayList<Future<Chunk>>();
        for (int i = 0; i < threadsNumber; i++) {
            final int idx = i;
            Future<Chunk> submit = ex.submit(new Callable<Chunk>() {
                public Chunk call() throws Exception {
                    final int start = fromIndex + chunkSize * idx;
                    if (idx == threadsNumber - 1) {
                        Arrays.sort(arr, start, len);
                    }
                    final int end = start + chunkSize - 1;
                    Arrays.sort(arr, start, end);
                    return null;

                }
            });
        }

    }

    public static void mergeSorted(int[] arr, int fromIndex, int toIndex, int splitIndex) {
        int[] result = new int[toIndex - fromIndex];
        int pos = 0;
        int li = fromIndex;
        int hi = splitIndex;
        while (li < splitIndex && hi < toIndex) {
            if (arr[li] <= arr[hi]) {
                result[pos++] = arr[li++];
            } else {
                result[pos++] = arr[hi++];
            }
        }
        if (li < splitIndex) {
            System.arraycopy(arr, li, arr, fromIndex + pos, splitIndex - pos);
        }
        System.arraycopy(result, 0, arr, fromIndex, pos);
    }
}
