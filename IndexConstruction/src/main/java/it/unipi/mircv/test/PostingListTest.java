package it.unipi.mircv.test;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.*;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class PostingListTest {

    /**
     * Write the object to a temporary test file and read to check if both the operations return the equal objects.
     */
    public static void readWriteTest(PostingList postingList) throws IOException {
        File folder = new File(IOUtils.PATH_TO_TEST);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        File testFile = new File (folder + "/PostingListTest.bin");

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        postingList.ToBinFile(bin_channel, false);
        bin_channel.close();
        bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.READ);
        PostingList test = new PostingList(postingList.getTerm());
        test.FromBinFile(bin_channel.position(0), false);
        bin_channel.close();

        assert test.getPl().size() == postingList.getPl().size() : "(ERROR) Posting List [From/To]BinFile(): The posting list wrote to file has" +
                "different dimension from the passed one. Maybe a compression problem?\n\n";

        postingList.initList();
        test.initList();
        while(postingList.getActualPosting() != null){
            assert test.getActualPosting() == postingList.getActualPosting() : "(ERROR) Posting List [From/To]BinFile(): Different wrote and read DocId found in posting list.\n\n";
            assert test.getActualPosting() == postingList.getActualPosting() : "(ERROR) Posting List [From/To]BinFile(): Different Frequency wrote and read found in posting list.\n\n";
            postingList.next();
            test.next();
        }

        testFile.delete();
    }

    /**
     * Test the initialization of the posting list iterator.
     */
    public static void testInitList(PostingList postingList) {
        postingList.initList();
        assert postingList.getActualPosting() == postingList.getPl().get(0) : "(ERROR) Posting List InitList(): Posting list iterator." +
                " not initialized correctly.\n\n";
    }

    /**
     * Test the nextGEQ() by searching and returning the found DocID and if it is greater or equal than the passed DocId
     */
    public static void testNextGEQ(PostingList postingList){
        int id = (int) Math.floor(Math.random()*postingList.getPl().get(postingList.getPl().size()-1).getDocId());
        postingList.nextGEQ(id);
        assert postingList.getActualPosting().getDocId() >= id : "(ERROR) Posting List NextGEQ(): Target DocId is not >= of passed random DocId." +
                " There could be a problem reading the SkipInfo File.\n\n";
    }

    /**
     * Test the readSkippingBlock() by checking if all the ordered posting of a given block are present in the posting list.
     */
    public static void readSkippingBlockTest(PostingList postingList) throws IOException {
        int index = (int) Math.floor(Math.random() * (postingList.getSkipList().getSl().size() - 1));
        SkipElem se = postingList.getSkipList().getSl().get(index);
        PostingList test = new PostingList(postingList.getTerm());
        test.readSkippingBlock(se);
        test.initList();
        postingList.initList();
        while(test.getActualPosting() != null){
            boolean found = false;
            while(postingList.getActualPosting() != null){
                if (test.getActualPosting() == postingList.getActualPosting()) {
                    found = true;
                }
                postingList.next();
            }
            assert found : "(ERROR) readSkippingBlock(): Posting List readSkippingBlock(): All the DocIds of a block were not found in the posting list.\n\n";
            test.next();
        }
    }

    /**
     * Execute the testing by passing a dummy posting list.
     */
    public static void doTest() throws IOException {

        //int size = InvertedIndex.getPosting_lists().size() - 1;
        //PostingList postingList = InvertedIndex.getPosting_lists().get((int) Math.floor(Math.random() * size));

        DictionaryElem de = InvertedIndex.getDictionary().get("000000000001");
        FileChannel channel = FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/indexMerged" + de.getBlock_number()+".bin"));
        PostingList postingList = IOUtils.readPlFromFile(channel, de.getOffset_block_pl(), "000000000001");
        assert postingList != null : "(ERROR) Read posting list is empty.";

        readWriteTest(postingList);
        testInitList(postingList);
        testNextGEQ(postingList);
        if(Flags.isSkipping())
            readSkippingBlockTest(postingList);
    }

}
