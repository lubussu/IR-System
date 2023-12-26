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

    /**
     * Execute the disjunctive query algorithm, with DAAT or MaxScore algorithms.
     * @param queryTerms Array list of the query's terms, in increasing length order
     * @return An array list of document scores ordered in decreasing order
     */
    public static PriorityQueue<DocumentScore> executeQueryProcesser(ArrayList<String> queryTerms){

        // Retrieve postingList of query terms
        for (int i=0; i<queryTerms.size();){
            String term = queryTerms.get(i);
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);

            // Term not in the Dictionary
            if(dict == null){
                queryTerms.remove(i);
                continue;
            }

            PostingList termPL;

            // Get the posting list offset in memory
            int offset = dict.getOffset_posting_lists();

            // If it is in memory read the posting
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset &&
                    InvertedIndex.getPosting_lists().get(offset).getTerm().equals(term)) {

                // If it is in memory read the posting list
                termPL = InvertedIndex.getPosting_lists().get(offset);
            }else{

                // Else read the posting list from file
                String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + dict.getBlock_number();
                FileChannel channel = IOUtils.getFileChannel(path, "read");
                if(Flags.isSkipping() && Flags.isMaxScore()){

                    // If there are skipping lists initialize the posting list with the first block
                    termPL = new PostingList(term);
                    SkipElem se = termPL.getSkipList().getSl().get(0);
                    termPL.readSkippingBlock(se);
                } else {

                    // Else read the full posting list from file and update the cache
                    termPL = IOUtils.readPlFromFile(channel, dict.getOffset_block_pl(), term);
                    InvertedIndex.updateCachePostingList(termPL, queryTerms);
                }
            }

            // Get the score to use for retrieval
            Double maxScore = Flags.isScoreMode() ? dict.getMaxBM25() : dict.getMaxTFIDF();
            termPL.initList();
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

            // If the using MaxScore order the posting lists as per the algorithm and execute it
            maxScores.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEach(entry -> {
                        orderedPLs.add(postingLists.get(entry.getKey()));
                        orderedMaxScores.add(entry.getValue());
                    });

            retrievedDocs = MaxScore.executeMaxScore(orderedPLs, orderedMaxScores);
        }else {

            // Else execute the DAAT algorithm
            retrievedDocs = DAAT.executeDAAT(postingLists);
        }
        clearLists();
        return retrievedDocs;
    }
}
