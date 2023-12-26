package it.unipi.mircv;

import it.unipi.mircv.utils.Flags;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class QueryTesting
{
    public static final String PATH_TO_QUERIES_TEST = "QueryTesting/src/main/resources/queries-test2020.tsv";

    /**
     * Executes the testing using the TREC-eval queries and results as reference.
     */
    public static void main( String[] args )
    {
        System.out.println("\n***** TESTING MODE *****\n");
        IndexConstruction.main(new String[]{"read"});

        // Print on terminal the parameter for testing
        Flags.printOption();

        long totTime = 0;
        int numQueries = 0;

        // Execute the retrieval for each query and print the obtained results
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(PATH_TO_QUERIES_TEST)), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                long start = System.currentTimeMillis();

                // EXECUTE QUERY .......
                QueryProcesser.executeQueryProcesser(line, true);

                long end = System.currentTimeMillis() - start;
                numQueries++;
                totTime += end;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.printf("\n(INFO) TESTING RESULTS PROCESSING %d QUERIES:\n", numQueries);
        System.out.printf("Total Time:   %ds\n", totTime/1000);
        System.out.printf("Average Time: %dms\n\n", totTime/numQueries);

    }
}
