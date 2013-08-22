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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        final ExecutorService ex = Executors.newFixedThreadPool(threadsNumber);

        class Chunk {

            public Chunk(int start, int end) {
                this.start = start;
                this.end = end;
            }
            int start, end;

            @Override
            public String toString() {
                return "Chunk{" + "start=" + start + ", end=" + end + '}';
            }
        }
        List<Future<Chunk>> futures = new ArrayList<Future<Chunk>>();
        for (int i = 0; i < threadsNumber; i++) {
            final int idx = i;
            futures.add(ex.submit(new Callable<Chunk>() {
                @Override
                public Chunk call() {
                    final int start = fromIndex + chunkSize * idx;
                    final int end = idx < threadsNumber - 1 ? start + chunkSize : fromIndex + len;
                    Arrays.sort(arr, start, end);
                    return new Chunk(start, end);
                }
            }));
        }

        while (futures.size() > 1) {
            final List<Future<Chunk>> tmp = new ArrayList<Future<Chunk>>();
            for (int i = 0; i < futures.size() / 2; i++) {
                final int idx = i;
                final Future<Chunk> firstFuture = futures.get(idx * 2);
                final Future<Chunk> secondFuture = futures.get(idx * 2 + 1);
                tmp.add(ex.submit(new Callable<Chunk>() {
                    @Override
                    public Chunk call() {
                        try {
                            Chunk first = firstFuture.get();
                            Chunk second = secondFuture.get();
                            assert first.end == second.start;
                            mergeSorted(arr, first.start, second.end, second.start);
                            return new Chunk(first.start, second.end);
                        } catch (Exception ex1) {
                            throw new RuntimeException(ex1);
                        }
                    }
                }));
            }
            if (futures.size() % 2 == 1) {
                tmp.add(futures.get(futures.size() - 1));
            }
            futures.clear();
            futures.addAll(tmp);
        }
        try {
            futures.get(0).get();
            ex.shutdown();
            ex.awaitTermination(10, TimeUnit.SECONDS);
        } catch (Exception ex1) {
            throw new RuntimeException(ex1);
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
            System.arraycopy(arr, li, arr, fromIndex + pos, splitIndex - li);
        }
        System.arraycopy(result, 0, arr, fromIndex, pos);

    }
}
