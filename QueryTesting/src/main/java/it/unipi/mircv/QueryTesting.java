package it.unipi.mircv;

import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.TextPreprocesser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;

public class QueryTesting
{
    public static final String PATH_TO_QUERIES_TEST = "QueryTesting/src/main/resources/queries-test2020.tsv";

    public static void main( String[] args )
    {
        System.out.println("\n***** TESTING MODE *****\n");
        IndexConstruction.main(new String[]{"read"});

        System.out.println("\nDEFAULT OPTIONS:");
        System.out.println("--------------------------------------------");
        System.out.printf("%-22s %4s %s\n", "Algorithm", " -> ", "MaxScore");
        System.out.printf("%-22s %4s %s\n", "Score", " -> ", "BM25");
        System.out.printf("%-22s %4s %s\n", "QueryMode", " -> ", "Disjunctive");
        System.out.printf("%-22s %4s %s\n", "Documents to retrieve", " -> ", "5");
        System.out.println("--------------------------------------------");

        ArrayList<String> tokens;
        long totTime = 0;
        int numQueries = 0;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(PATH_TO_QUERIES_TEST)), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                long start = System.currentTimeMillis();
                tokens = TextPreprocesser.executeTextPreprocessing(line);
                if (tokens.size() == 0) {
                    System.out.println("(ERROR) Query not valid!");
                    continue;
                }

                // EXECUTE QUERY .......
                QueryProcesser.executeQueryProcesser(tokens, 5, true);

                long end = System.currentTimeMillis() - start;
                numQueries++;
                totTime += end;
                tokens.clear();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.printf("\n(INFO) TESTING RESULTS PROCESSING %d QUERIES:\n", numQueries);
        System.out.printf("Total Time:   %ds\n", totTime/1000);
        System.out.printf("Average Time: %dms\n\n", totTime/numQueries);

    }
}
