package it.unipi.mircv;

import it.unipi.mircv.algorithm.ConjunctiveQuery;
import it.unipi.mircv.algorithm.DisjunctiveQuery;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.Flags;

import java.nio.channels.FileChannel;
import java.util.*;

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

    public static void executeQueryProcesser(ArrayList<String> queryTerms, boolean testing){
        PriorityQueue<DocumentScore> retrievedDocs;

        if(Flags.isDisjunctive()) {
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
                    if(Flags.isSkipping()){
                        termPL = new PostingList(term);
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
                return;
            }
            if (Flags.isMaxScore()) {

                maxScores.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .forEach(entry -> {
                            orderedPLs.add(postingLists.get(entry.getKey()));
                            orderedMaxScores.add(entry.getValue());
                        });

                retrievedDocs = DisjunctiveQuery.executeMaxScore(orderedPLs, orderedMaxScores);
            }else {
                retrievedDocs = DisjunctiveQuery.executeDAAT(postingLists);
            }

        } else { //Conjunctive Mode
            PriorityQueue<DictionaryElem> term_df_inc = new PriorityQueue<>(queryTerms.size(), ((a, b) -> a.compareTo(b)));
            for (String term: queryTerms){
                DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                if(dict == null){
                    break;
                }
                term_df_inc.add(dict);
            }

            if(term_df_inc.size() != queryTerms.size()) {
                System.out.println("(ERROR) No documents found\n");
                return;
            }

            DictionaryElem first_dict = term_df_inc.poll();
            String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + first_dict.getBlock_number();
            FileChannel channel = IOUtils.getFileChannel(path, "read");
            PostingList pl = IOUtils.readPlFromFile(channel, first_dict.getOffset_block_pl(), first_dict.getTerm());
            postingLists.add(pl);
            while(!term_df_inc.isEmpty()){
                pl = new PostingList(term_df_inc.poll().getTerm());
                postingLists.add(pl);
            }
            retrievedDocs = ConjunctiveQuery.executeConjunctiveQuery(postingLists);
        }

        clearLists();

        if(testing) //don't print if testing mode
            return;

        ArrayList<Integer> topKResults = new ArrayList<>();
        StringBuilder results = new StringBuilder();
        while(!retrievedDocs.isEmpty()){
            DocumentScore ds = retrievedDocs.poll();
            results.insert(0, "Document: " + ds.getDocId() + "\t\tScore: " + ds.getScore() + "\n");
            topKResults.add(0, ds.getDocId());
        }
        System.out.println(results);
    }

    public static void executeQueryProcesser2(ArrayList<String> queryTerms, boolean testing){
        // Ottenere le posting list dei termini nella query
        PriorityQueue<DictionaryElem> term_df_inc = new PriorityQueue<>(queryTerms.size(), ((a, b) -> a.compareTo(b)));
        for (int i=0; i<queryTerms.size();){
            String term = queryTerms.get(i);
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);

            if(dict == null){//term not found
                if(Flags.isDisjunctive()){
                    queryTerms.remove(i);
                    continue;
                } else{
                    break;
                }
            }

            if(!Flags.isDisjunctive()){
                term_df_inc.add(dict);
                continue;
            }

            PostingList termPL;
            int offset = dict.getOffset_posting_lists();
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(term)) {
                termPL = InvertedIndex.getPosting_lists().get(offset);
            }else{
                String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + dict.getBlock_number();
                FileChannel channel = IOUtils.getFileChannel(path, "read");
                if(Flags.isSkipping()){
                    termPL = new PostingList(term);
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
            return;
        }

        PriorityQueue<DocumentScore> retrievedDocs = null;
        if (Flags.isDisjunctive() && Flags.isMaxScore()) {

            maxScores.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEach(entry -> {
                        orderedPLs.add(postingLists.get(entry.getKey()));
                        orderedMaxScores.add(entry.getValue());
                    });

            retrievedDocs = DisjunctiveQuery.executeMaxScore(orderedPLs, orderedMaxScores);
        }else if (Flags.isDisjunctive() && !Flags.isMaxScore()) {
            retrievedDocs = DisjunctiveQuery.executeDAAT(postingLists);
        } else if (!Flags.isDisjunctive()){
            DictionaryElem first_dict = term_df_inc.poll();
            String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + first_dict.getBlock_number();
            FileChannel channel = IOUtils.getFileChannel(path, "read");
            PostingList pl = IOUtils.readPlFromFile(channel, first_dict.getOffset_block_pl(), first_dict.getTerm());
            postingLists.add(pl);
            while(!term_df_inc.isEmpty()){
                pl = new PostingList(term_df_inc.poll().getTerm());
                postingLists.add(pl);
            }
            retrievedDocs = ConjunctiveQuery.executeConjunctiveQuery(postingLists);
        }

        clearLists();

        if(testing) //don't print if testing mode
            return;

        ArrayList<Integer> topKResults = new ArrayList<>();
        StringBuilder results = new StringBuilder();
        while(!retrievedDocs.isEmpty()){
            DocumentScore ds = retrievedDocs.poll();
            results.insert(0, "Document: " + ds.getDocId() + "\t\tScore: " + ds.getScore() + "\n");
            topKResults.add(0, ds.getDocId());
        }
        System.out.println(results);
    }
}
