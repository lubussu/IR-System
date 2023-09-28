package it.unipi.mircv.Utils;

import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class IOUtils {

    public static boolean writeIndexBlockToDisk(HashMap<String, DictionaryElem> blockDictionary,
                                                HashMap<String, PostingList> blockPostingList, int block) throws IOException{

        for (String term: blockDictionary.keySet()){

            PostingList block_pl = blockPostingList.get(term);
            DictionaryElem block_de = blockDictionary.get(term);

            block_pl.ToTextFile("temp/indexBlock" + block + ".txt");
            block_de.ToTextFile("temp/dictionaryBlock" + block + ".txt");

        }
        return true;
    }



}
