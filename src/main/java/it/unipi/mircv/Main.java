package it.unipi.mircv;

import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.TextPreprocesser;

import java.util.*;

public class Main {

    public static void setOptions(){
        Scanner sc = new Scanner(System.in);
        String type;
        do {
            System.out.println("SELECT QUERY TYPE:\n1: To execute Conjunctive Query\n2: To execute Disjunctive Query");
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));
        Flags.setQueryMode(type.equals("1"));

        do {
            System.out.println("SELECT EXECUTION MODE:\n1: To execute DAAT\n2: To execute MaxScore");
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));
        Flags.setMaxScore(type.equals("2"));

        do {
            System.out.println("\nSELECT SCORE:\n1 -> To use TFIDF\n2 -> To use BM25");
            type = sc.nextLine();
        } while (!type.equals("1") && !type.equals("2"));
        Flags.setScoreMode(type.equals("2"));
    }

    public static void main(String[] args){

        System.out.println("\n***** SEARCH ENGINE *****\n");

        String query;
        ArrayList<String> tokens;
        IndexConstruction.main(new String[]{"read"});

        Scanner sc = new Scanner(System.in);
        String type;
        int k = 5;

        System.out.println("DEFAULT OPTIONS:");
        System.out.println("--------------------------------------------");
        System.out.printf("%-22s %4s %s\n", "Algorithm", " -> ", "MaxScore");
        System.out.printf("%-22s %4s %s\n", "Score", " -> ", "BM25");
        System.out.printf("%-22s %4s %s\n", "QueryMode", " -> ", "Disjunctive");
        System.out.printf("%-22s %4s %s\n", "Documents to retrieve", " -> ", "5");
        System.out.println("--------------------------------------------");
        System.out.println("Do you want to change? (y/n)");
        type = sc.nextLine();

        while (true){
            if(type.equals("y")){
                setOptions();
                do {
                    System.out.println("Insert the number of document to retrieve: ");
                    type = sc.nextLine();

                    try {
                        k = Integer.parseInt(type);
                        break;
                    } catch (NumberFormatException ex){
                        System.out.println("(ERROR) Number not valid.");
                    }
                } while (true);
            }

            /* Insert the query */
            System.out.println("\nInsert Query or select option:\n'exit' -> To terminate\n'back' -> To change options");
            query = sc.nextLine();

            if (query == null || query.isEmpty()){
                System.out.println("(ERROR) Insert a correct query.");
                continue;
            }
            else if (query.equals("exit"))
                return;
            else if (query.equals("back")) {
                type = "y";
                continue;
            }

            long start = System.currentTimeMillis();
            tokens = TextPreprocesser.executeTextPreprocessing(query);
            if (tokens.size() == 0) {
                System.out.println("(ERROR) Query not valid!");
                continue;
            }

            // EXECUTE QUERY .......
            QueryProcesser.executeQueryProcesser(tokens, k, false);

            long end = System.currentTimeMillis() - start;
            System.out.println("\n(INFO) Query executed in: " + end + " ms");
            tokens.clear();
            type="";
        }

    }

}
