package it.unipi.mircv;

import it.unipi.mircv.algorithm.DAAT;
import it.unipi.mircv.utils.Flags;

import java.util.*;

public class Main {

    public static void main(String[] args){

        System.out.println("***** SEARCH ENGINE *****");

        String query;
        ArrayList<String> tokens;
        IndexConstruction.main(new String[]{"read"});

        String type;
        int k = 1;
        boolean goOn = false;

        while (true) {
            /* Insert the query */
            System.out.println("\nInsert Query or 'exit' command to terminate: ");
            Scanner sc = new Scanner(System.in);
            query = sc.nextLine();

            if (query == null || query.isEmpty()){
                System.out.println("(ERROR) Insert a correct query.");
                continue;
            }
            else if (query.equals("exit"))
                break;

            tokens = TextPreprocesser.executeTextPreprocessing(query);
            if (tokens.size() == 0) {
                System.out.println("(ERROR) Query not valid!");
                continue;
            }

            do {
                System.out.println("SELECT QUERY TYPE:\n1: To execute Conjunctive Query\n2: To execute Disjunctive Query");
                sc = new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2"));
            Flags.setQueryMode(type.equals("1"));

            do {
                System.out.println("SELECT EXECUTION MODE:\n1: To execute DAAT\n2: To execute MaxScore");
                sc=new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2"));
            Flags.setMaxScore(type.equals("2"));

            do {
                System.out.println("\nSELECT SCORE:\n1 -> To use TFIDF\n2 -> To use BM25");
                sc = new Scanner(System.in);
                type = sc.nextLine();
            } while (!type.equals("1") && !type.equals("2"));
            Flags.setScoreMode(type.equals("2"));

            do {
                System.out.println("Insert the number of document to retrieve: ");
                sc = new Scanner(System.in);
                type = sc.nextLine();

                try {
                    k = Integer.parseInt(type);
                    goOn = true;
                } catch (NumberFormatException ex){
                    System.out.println("(ERROR) Number not valid.");
                }
            } while (!goOn);

            long start = System.currentTimeMillis();

            // EXECUTE QUERY .......
            DAAT.retrieveDocuments(tokens, k);

            long end = System.currentTimeMillis() - start;
            System.out.println("\n(INFO) Query executed in: " + end + " ms");
            tokens.clear();
        }
    }
}
