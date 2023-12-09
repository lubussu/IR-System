package it.unipi.mircv.test;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.SkipList;
import it.unipi.mircv.utils.Flags;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SkipListTest {

    public static void readWriteTest(SkipList skipList) throws IOException {
        File folder = new File("test");
        if (!folder.exists()) {
            folder.mkdirs();
        }

        File testFile = new File (folder + "/SkipListTest.bin");

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        skipList.ToBinFile(bin_channel);
        bin_channel.close();
        bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.READ);
        SkipList test = new SkipList(skipList.getTerm());
        test.FromBinFile(bin_channel.position(0));
        bin_channel.close();

        for(int i = 0; i < skipList.getSl().size(); i++){
            assert test.getSl().get(i).getMaxDocId() == skipList.getSl().get(i).getMaxDocId() : "(ERROR) Skipping List [From/To]BinFile(): Different wrote and read Block max DocId found in skipping list.\n\n";
            assert test.getSl().get(i).getBlock_size() == skipList.getSl().get(i).getBlock_size() : "(ERROR) Skipping List [From/To]BinFile(): Different wrote and read Block size found in skipping list.\n\n";
            assert test.getSl().get(i).getBlockStartingOffset() == skipList.getSl().get(i).getBlockStartingOffset() : "(ERROR) Skipping List [From/To]BinFile(): Different wrote and read Block starting offset found in skipping list.\n\n";
        }
    }

    public static void doTest() throws IOException {

        if(Flags.isSkipping()) {
            int size = InvertedIndex.getSkip_lists().size() - 1;
            SkipList skipList = InvertedIndex.getSkip_lists().get((int) Math.floor(Math.random() * size));

            readWriteTest(skipList);
        }

    }

}
