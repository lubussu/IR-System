package it.unipi.mircv.test;

import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.PostingList;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CollectionInfoTest {

    public static void readWriteTest() throws IOException {
        File folder = new File("/test");
        if (!folder.exists()) {
            folder.mkdirs();
        }

        File testFile = new File (folder + "CollectionInfoTest.bin");

        long collectionSize = CollectionInfo.getCollection_size();
        long collectionTotalLen = CollectionInfo.getCollection_total_len();

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        CollectionInfo.ToBinFile(bin_channel);
        CollectionInfo.FromBinFile(bin_channel.position(0));

        assert collectionSize == CollectionInfo.getCollection_size() : "(ERROR) Collection Info [From/To]BinFile(): different wrote and read Collection size.\n\n";
        assert collectionTotalLen == CollectionInfo.getCollection_total_len() : "(ERROR) Collection Info [From/To]BinFile(): different wrote and read Collection total length.\n\n";

        testFile.delete();
    }

    public static void doTest() throws IOException {

        readWriteTest();

    }

}
