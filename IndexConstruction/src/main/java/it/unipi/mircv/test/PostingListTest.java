package it.unipi.mircv.test;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.SkipElem;
import it.unipi.mircv.utils.Flags;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class PostingListTest {

    public static void readWriteTest(PostingList postingList) throws IOException {
        File folder = new File("/test");
        if (!folder.exists()) {
            folder.mkdirs();
        }

        File testFile = new File (folder + "PostingListTest.bin");

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        postingList.ToBinFile(bin_channel, false);
        PostingList test = new PostingList(postingList.getTerm());
        test.FromBinFile(bin_channel.position(0), false);

        assert test.getPl().size() == postingList.getPl().size() : "(ERROR) Posting List [From/To]BinFile(): The posting list wrote to file has" +
                "different dimension from the passed one. Maybe a compression problem?\n\n";

        for(int i = 0; i < postingList.getPl().size(); i++){
            assert test.getPl().get(i).getDocId() == postingList.getPl().get(i).getDocId() : "(ERROR) Posting List [From/To]BinFile(): Different wrote and read DocId found in posting list.\n\n";
            assert test.getPl().get(i).getTermFreq() == postingList.getPl().get(i).getTermFreq() : "(ERROR) Posting List [From/To]BinFile(): Different Frequency wrote and read found in posting list.\n\n";
        }

        testFile.delete();
    }

    public static void testInitList(PostingList postingList) {
        postingList.initList();
        assert postingList.getActualPosting() == postingList.getPl().get(0) : "(ERROR) Posting List InitList(): Posting list iterator." +
                " not initialized correctly.\n\n";
    }

    public static void testNextGEQ(PostingList postingList){
        int id = (int) Math.floor(Math.random()*postingList.getPl().get(postingList.getPl().size()-1).getDocId());
        postingList.nextGEQ(id);
        assert postingList.getActualPosting().getDocId() >= id : "(ERROR) Posting List NextGEQ(): Target DocId is not >= of passed random DocId." +
                " There could be a problem reading the SkipInfo File.\n\n";
    }

    public static void readSkippingBlockTest(PostingList postingList) throws IOException {
        int size = (int) Math.floor(Math.random() * (postingList.getSkipList().getSl().size() - 1));
        SkipElem se = postingList.getSkipList().getSl().get(size);
        PostingList test = new PostingList(postingList.getTerm());
        test.readSkippingBlock(se);
        for(Posting post_test : test.getPl()){
            boolean found = false;
            for(Posting post : postingList.getPl()){
                if (post_test.getDocId() == post.getDocId()) {
                    found = true;
                }
            }
            assert found : "(ERROR) readSkippingBlock(): Posting List readSkippingBlock(): All the DocIds of a block were not found in the posting list.\n\n";
        }
    }

    public static void doTest() throws IOException {

        int size = InvertedIndex.getPosting_lists().size() - 1;
        PostingList postingList = InvertedIndex.getPosting_lists().get((int) Math.floor(Math.random() * size));

        readWriteTest(postingList);
        testInitList(postingList);
        testNextGEQ(postingList);
        if(Flags.isSkipping())
            readSkippingBlockTest(postingList);
    }

}
