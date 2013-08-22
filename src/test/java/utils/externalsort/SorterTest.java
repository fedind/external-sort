package utils.externalsort;

import java.util.Arrays;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author fedin
 */
public class SorterTest {

    @Test
    public void testParallelSort() {
        for (int threadNumber = 1; threadNumber < 9; threadNumber++) {
            System.out.println("threadNumber = " + threadNumber);
            int[] arr = new int[100];
            Random r = new Random();
            for (int i = 0; i < arr.length; i++) {
                arr[i] = r.nextInt(100);
            }
            System.out.format("arr: %s\n", Arrays.toString(arr));

            int[] expected = new int[arr.length];
            System.arraycopy(arr, 0, expected, 0, arr.length);
            Arrays.sort(expected);
            Sorter.parallelSort(arr, 0, arr.length, threadNumber);
            System.out.format("result: %s\n", Arrays.toString(arr));
            Assert.assertArrayEquals(arr, expected);
        }
    }

    /**
     * Test of mergeSorted method, of class Sorter.
     */
    @Test
    public void testMergeSorted() {
        System.out.println("mergeSorted");
        testArray(new int[]{1, 10, 12, 13, 14, 15, 16, 2, 4, 6, 15, 20, 21}, 2);
        testArray(new int[]{4, 6, 15, 20, 21, 0, 1, 10, 12, 13, 14, 15, 16}, 0);
        testArray(new int[]{1, 10, 12, 13, 14, 2, 4, 6, 15, 20, 21}, 2);
        
        System.out.println("success");
    }

    private void testArray(int[] arr, final int splitValue) {
        System.out.format("tested array: %s\n", Arrays.toString(arr));
        int[] result = new int[arr.length];
        System.arraycopy(arr, 0, result, 0, arr.length);
        Arrays.sort(result);
        System.out.format("expected array: %s\n", Arrays.toString(result));
        int splitIndex = -1;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == splitValue) {
                splitIndex = i;
                break;
            }
        }

        Sorter.mergeSorted(arr, 0, arr.length, splitIndex);
        System.out.format("result: %s\n", Arrays.toString(arr));
        Assert.assertArrayEquals(arr, result);
    }
}