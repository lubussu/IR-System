package it.unipi.mircv;

import it.unipi.mircv.algorithm.DAAT;
import it.unipi.mircv.algorithm.MaxScore;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.SkipElem;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class DisjunctiveQuery {
    private static ArrayList<PostingList> postingLists = new ArrayList<>();
    private static HashMap<Integer, Double> maxScores = new HashMap<>();
    public static ArrayList<PostingList> orderedPLs = new ArrayList<>();
    public static ArrayList<Double> orderedMaxScores = new ArrayList<>();

    public static void clearLists(){
        postingLists.clear();
        maxScores.clear();
        orderedPLs.clear();
        orderedMaxScores.clear();
    }

    public static PriorityQueue<DocumentScore> executeQueryProcesser(ArrayList<String> queryTerms){

        //Retrieve postingList of query terms
        for (int i=0; i<queryTerms.size();){
            String term = queryTerms.get(i);
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);

            if(dict == null){ //term not in the Dictionary
                queryTerms.remove(i);
                continue;
            }

            PostingList termPL;
            int offset = dict.getOffset_posting_lists();
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(term)) {
                termPL = InvertedIndex.getPosting_lists().get(offset);
            }else{ //read PostingList from file
                String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + dict.getBlock_number();
                FileChannel channel = IOUtils.getFileChannel(path, "read");
                if(Flags.isSkipping() && Flags.isMaxScore()){
                    termPL = new PostingList(term);
                    SkipElem se = termPL.getSkipList().getSl().get(0);
                    termPL.readSkippingBlock(se);
                } else {
                    termPL = IOUtils.readPlFromFile(channel, dict.getOffset_block_pl(), term);
                    InvertedIndex.updateCachePostingList(termPL, queryTerms);
                }
            }
            Double maxScore = Flags.isScoreMode() ? dict.getMaxBM25() : dict.getMaxTFIDF();
            termPL.initList(); //posiziona l'iteratore all'inizio
            postingLists.add(termPL);
            maxScores.put(i, maxScore);
            i++;
        }
        if(postingLists.size()==0) {
            System.out.println("(ERROR) No documents found\n");
            return new PriorityQueue<>();
        }

        PriorityQueue<DocumentScore> retrievedDocs;
        if (Flags.isMaxScore()) {
            maxScores.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEach(entry -> {
                        orderedPLs.add(postingLists.get(entry.getKey()));
                        orderedMaxScores.add(entry.getValue());
                    });

            retrievedDocs = MaxScore.executeMaxScore(orderedPLs, orderedMaxScores);
        }else {
            retrievedDocs = DAAT.executeDAAT(postingLists);
        }
        clearLists();
        return retrievedDocs;
    }
}
