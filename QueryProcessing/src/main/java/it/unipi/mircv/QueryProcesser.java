package it.unipi.mircv;

import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.algorithm.DAAT;
import it.unipi.mircv.algorithm.MaxScore;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.utils.Flags;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class QueryProcesser {
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

    public static void executeQueryProcesser(ArrayList<String> queryTerms, int k, boolean testing){
        // Ottenere le posting list dei termini nella query

        for (int i=0; i<queryTerms.size();){
            String term = queryTerms.get(i);
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);
            if(dict == null) { //term not found
                queryTerms.remove(i);
                continue;
            }

            PostingList termPL;
            int offset = dict.getOffset_posting_lists();
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(term)) {
                termPL = InvertedIndex.getPosting_lists().get(offset);
            }else{
                String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + dict.getBlock_number();
                FileChannel channel = IOUtils.getFileChannel(path, "read");
                termPL = IOUtils.readPlFromFile(channel, dict.getOffset_block(), term);
                InvertedIndex.updateCachePostingList(termPL, queryTerms);
            }
            Double maxScore = Flags.isScoreMode() ? dict.getMaxBM25() : dict.getMaxTFIDF();
            termPL.initList(); //posiziona l'iteratore all'inizio
            postingLists.add(termPL);
            maxScores.put(i, maxScore);
            i++;
        }

        if(postingLists.size()==0) {
            System.out.println("(ERROR) No documents found\n");
            return;
        }

        PriorityQueue<DocumentScore> retrievedDocs;
        if (Flags.isMaxScore()) {

            maxScores.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEach(entry -> {
                        orderedPLs.add(postingLists.get(entry.getKey()));
                        orderedMaxScores.add(entry.getValue());
                    });

            retrievedDocs = MaxScore.executeMaxScore(orderedPLs, orderedMaxScores, k);
        }else {
            retrievedDocs = DAAT.executeDAAT(postingLists, k);
        }
        clearLists();
        if(testing) //don't print if testing mode
            return;

        ArrayList<Integer> topKResults = new ArrayList<>();
        String results = "";
        while(!retrievedDocs.isEmpty()){
            DocumentScore ds = retrievedDocs.poll();
            results = "Document: "+ds.getDocId()+"\t\tScore: "+ds.getScore()+"\n" + results;
            topKResults.add(0, ds.getDocId());
        }
        System.out.println(results);
    }
}
