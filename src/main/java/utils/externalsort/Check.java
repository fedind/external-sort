package utils.externalsort;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author fedin
 */
public class Check {

    public static void main(String[] args) throws IOException {
        File infile = new File("\\temp\\text.dat");
        File outfile = new File("\\temp\\out.dat");
        RandomAccessFile in = null;
        DataInputStream out = null;
        try {
            in = new RandomAccessFile(infile, "r");
            out = new DataInputStream(new BufferedInputStream(new FileInputStream(outfile)));
            for (long i = in.length() - Utils.BYTE_TO_INT; i > 0; i -= Utils.BYTE_TO_INT) {
                in.seek(i);
                int inN = in.readInt();
                int outN = out.readInt();
                if (inN!=outN) {
                    throw new IllegalStateException("error at " + i + " position: " + inN + ", " + outN);
                }
                
            }

        } finally {
            Utils.close(in);
            Utils.close(out);
        }

    }
}
