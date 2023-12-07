package it.unipi.mircv;

import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.TextPreprocesser;

import java.util.*;

public class QueryProcesser {

    public static void executeQueryProcesser(String query, boolean testing){
        long start = System.currentTimeMillis();
        ArrayList<String> queryTerms = TextPreprocesser.executeTextPreprocessing(query);
        if (queryTerms.size() == 0) {
            System.out.println("(ERROR) Query not valid!");
            return;
        }
        if(testing){
            queryTerms.remove(0); //remove line number
        }
        PriorityQueue<DocumentScore> retrievedDocs;

        if(Flags.isConjunctive()) {
            retrievedDocs = ConjunctiveQuery.executeQueryProcesser(queryTerms);
        } else {
            retrievedDocs = DisjunctiveQuery.executeQueryProcesser(queryTerms);
        }

        long end = System.currentTimeMillis() - start;

        if(!testing) { //don't print results if testing mode
            System.out.println("\n(INFO) Query executed in: " + end + " ms");

            ArrayList<Integer> topKResults = new ArrayList<>();
            StringBuilder results = new StringBuilder();
            while (!retrievedDocs.isEmpty()) {
                DocumentScore ds = retrievedDocs.poll();
                results.insert(0, "Document: " + ds.getDocId() + "\t\tScore: " + ds.getScore() + "\n");
                topKResults.add(0, ds.getDocId());
            }
            System.out.println(results);
        }
    }

}
