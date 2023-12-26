package it.unipi.mircv.test;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DocumentElemTest {

    /**
     * Write the object to a temporary test file and read to check if both the operations return the equal objects.
     */
    public static void readWriteTest(DocumentElem documentElem) throws IOException {
        File folder = new File(IOUtils.PATH_TO_TEST);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        File testFile = new File (folder + "/DocumentElemTest.bin");

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        documentElem.ToBinFile(bin_channel);
        bin_channel.close();
        DocumentElem test = new DocumentElem();
        bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.READ);
        test.FromBinFile(bin_channel);
        bin_channel.close();
        assert documentElem.getDocNo().equals(test.getDocNo()) : "(ERROR) Document Element [From/To]BinFile: different wrote and read Document number.\n\n";
        assert documentElem.getDocId() == test.getDocId() : "(ERROR) Document Element [From/To]BinFile: different wrote and read Document Id.\n\n";
        assert documentElem.getLength() == test.getLength() : "(ERROR) Document Element [From/To]BinFile: different wrote and read Document length.\n\n";

        testFile.delete();
    }

    /**
     * Takes a random dummy document element to execute the test.
     */
    public static void doTest() throws IOException {

        int size = InvertedIndex.getDocTable().size() - 1;
        DocumentElem documentElem = InvertedIndex.getDocTable().get((int) Math.floor(Math.random() * size));

        readWriteTest(documentElem);

    }

}
