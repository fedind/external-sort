package utils.externalsort;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author fedin
 */
public class TestGenerator {

    public static final int SIZE = 80000028;

    public static void main(String[] args) throws FileNotFoundException, IOException {
        final String fileName = "\\temp\\text.dat";
        System.out.format("start: fileName = %s, SIZE = %dkB\n", fileName, SIZE/1042);
        Timer timer = Timer.start();
        Random rand = new Random();
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
            final int N = SIZE / Utils.BYTE_TO_INT;                
            System.out.println("N = " + N);
            for (int i = N; i > 0; i--) {                
                out.writeInt(rand.nextInt());
            }
        } finally {
            Utils.close(out);
        }
        File file = new File(fileName);
        System.out.println(timer);
        System.out.format("%s file is %,dKB size\n", fileName, file.length() / 1024);
        System.out.println("finished");
    }
}
