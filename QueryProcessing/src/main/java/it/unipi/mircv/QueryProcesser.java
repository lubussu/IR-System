package it.unipi.mircv;

import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.TextPreprocesser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class QueryProcesser {

    /**
     * Computes the preprocessing of the query needed to start the query execution.
     * @param query String containing the query
     * @param testing Boolean marking if the query is in test mode
     */
    public static void executeQueryProcesser(String query, boolean testing){
        String fix = "Q0";
        String runid = "RUN-01";

        long start = System.currentTimeMillis();

        // Execute the query preprocessing
        ArrayList<String> queryTerms = TextPreprocesser.executeTextPreprocessing(query);
        if (queryTerms.size() == 0) {
            System.out.println("(ERROR) Query not valid!");
            return;
        }
        String topicId = "";
        if(testing){
            // Remove line number
            topicId = queryTerms.remove(0);
        }
        PriorityQueue<DocumentScore> retrievedDocs;

        // Execute conjunctive or disjunctive query based on the isConjunctive() flag
        if(Flags.isConjunctive()) {
            retrievedDocs = ConjunctiveQuery.executeQueryProcesser(queryTerms);
        } else {
            retrievedDocs = DisjunctiveQuery.executeQueryProcesser(queryTerms);
        }

        long end = System.currentTimeMillis() - start;

        int rank = 1;

        ArrayList<Integer> topKResults = new ArrayList<>();
        StringBuilder results = new StringBuilder();
        StringBuilder test_results = new StringBuilder();

        // Print the scores on the terminal
        while (!retrievedDocs.isEmpty()) {
            DocumentScore ds = retrievedDocs.poll();
            results.insert(0, "Document: " + ds.getDocId() + "\t\tScore: " + ds.getScore() + "\n");
            test_results.append(topicId + "\t" + fix + "\t" + ds.getDocId()+ "\t" + rank + "\t" + (Math.floor(ds.getScore() * 1000) / 1000) + "\t" + runid + "\n");

            topKResults.add(0, ds.getDocId());
            rank ++;
        }
        // Don't print results if testing mode
        if(!testing) {
            System.out.println("\n(INFO) Query executed in: " + end + " ms");
            System.out.println(results);
        } else {
            File file = new File("results.txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                writer.write(String.valueOf(test_results));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
