package it.unipi.mircv.utils;

public class Flags {

    // Building parameters
    private static boolean compression = true;
    private static boolean skipping = true;

    //Query parameters

    /* TRUE: MaxScore  FALSE: DAAT */
    private static boolean maxScore = true;

    /* TRUE: BM25  FALSE: TFIDF */
    private static boolean scoreMode = true;

    /* TRUE: Conjunctive Query  FALSE: Disjunctive Query */
    private static boolean disjunctive = false;

    private static int numDocs = 5;

    /* SETTER AND GETTER SECTION */

    public static void setCompression(boolean compression) {
        Flags.compression = compression;
    }

    public static void setMaxScore(boolean maxScore) {
        Flags.maxScore = maxScore;
    }

    public static void setDisjunctive(boolean disjunctive) {Flags.disjunctive = disjunctive;    }

    public static void setScoreMode(boolean scoreMode) { Flags.scoreMode = scoreMode;}

    public static void setSkipping(boolean skipping) { Flags.skipping = skipping; }

    public static void setNumDocs(int numDocs) {
        Flags.numDocs = numDocs;
    }

    public static int getNumDocs() {
        return numDocs;
    }

    public static boolean isCompression() {
        return compression;
    }

    public static boolean isScoreMode() {return scoreMode;}

    public static boolean isMaxScore() {
        return maxScore;
    }

    public static boolean isDisjunctive() {return disjunctive;}

    public static boolean isSkipping() {return skipping;}

    public static void printOption(){
        System.out.println("DEFAULT OPTIONS:");
        System.out.println("--------------------------------------------");
        System.out.printf("%-22s %4s %s\n", "Algorithm", " -> ", isMaxScore()?"MaxScore":"DAAT");
        System.out.printf("%-22s %4s %s\n", "Score", " -> ", isScoreMode()?"BM25":"TFIDF");
        System.out.printf("%-22s %4s %s\n", "QueryMode", " -> ", isDisjunctive()?"Conjunctive":"Disjunctive");
        System.out.printf("%-22s %4s %s\n", "SkippingMode", " -> ", isSkipping()?"True":"False");
        System.out.printf("%-22s %4s %d\n", "Documents to retrieve", " -> ", getNumDocs());
        System.out.println("--------------------------------------------");
    }

}
