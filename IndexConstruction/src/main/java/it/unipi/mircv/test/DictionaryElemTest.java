package it.unipi.mircv.test;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.DocumentElem;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DictionaryElemTest {

    public static void readWriteTest(DictionaryElem dictionaryElem, String term) throws IOException {
        File folder = new File("/test");
        if (!folder.exists()) {
            folder.mkdirs();
        }
        File testFile = new File (folder + "DictionaryElemTest.bin");

        FileChannel bin_channel = FileChannel.open(Paths.get(testFile.toURI()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        dictionaryElem.ToBinFile(bin_channel);
        DictionaryElem test = new DictionaryElem(term);
        test.FromBinFile(bin_channel.position(0));
        assert dictionaryElem.getDf() == test.getDf() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Document frequency.\n\n";
        assert dictionaryElem.getCf() == test.getCf() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Collection frequency.\n\n";
        assert dictionaryElem.getBlock_number() == test.getBlock_number() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Block number.\n\n";
        assert dictionaryElem.getOffset_block_pl() == test.getOffset_block_pl() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Offset in block number.\n\n";
        assert dictionaryElem.getBlock_number() == test.getBlock_number() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Block number.\n\n";
        assert dictionaryElem.getMaxTf() == test.getMaxTf() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Max term frequency.\n\n";
        assert dictionaryElem.getIdf() == test.getIdf() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Inverse document frequency.\n\n";
        assert dictionaryElem.getMaxTFIDF() == test.getMaxTFIDF() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Max TFIDF score.\n\n";
        assert dictionaryElem.getMaxBM25() == test.getMaxBM25() : "(ERROR) Dictionary Element [From/To]BinFile(): different wrote and read Max BM25 score.\n\n";

        testFile.delete();
    }

    public static void doTest() throws IOException {

        int size = InvertedIndex.getTermList().size() - 1;
        String term;
        if(InvertedIndex.getTermList().isEmpty())
            term = "Hello";
        else
            term = InvertedIndex.getTermList().get((int) Math.floor(Math.random() * size));
        DictionaryElem dictionaryElem = InvertedIndex.getDictionary().get(term);

        readWriteTest(dictionaryElem, term);

    }

}
