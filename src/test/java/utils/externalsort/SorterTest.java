package utils.externalsort;

import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author fedin
 */
public class SorterTest {

    /**
     * Test of parallelSort method, of class Sorter.
     */
    @Test
    public void testParallelSort() {
        System.out.println("parallelSort");
        int[] arr = null;
        int fromIndex = 0;
        int toIndex = 0;
        int threadsNumber = 1;
        Sorter.parallelSort(arr, fromIndex, toIndex, threadsNumber);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of mergeSorted method, of class Sorter.
     */
    @Test
    public void testMergeSorted() {
        System.out.println("mergeSorted");
        final int SPLIT_VALUE = 2;
        int[] arr = {1, 10, 12, 13, 14, 15, 16, SPLIT_VALUE, 4, 6, 15, 20, 21};
        System.out.format("tested array: %s\n", Arrays.toString(arr));
        int[] result = new int[arr.length];
        System.arraycopy(arr, 0, result, 0, arr.length);
        Arrays.sort(result);
        System.out.format("expected array: %s\n", Arrays.toString(result));
        int splitIndex = -1;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == SPLIT_VALUE) {
                splitIndex = i;
                break;
            }
        }

        Sorter.mergeSorted(arr, 0, arr.length, splitIndex);
        System.out.format("result: %s\n", Arrays.toString(arr));
        Assert.assertArrayEquals(arr, result);

        System.out.println("success");
    }
}