package it.unipi.mircv.utils;

public class Flags {

    // Building parameters
    private static boolean compression = true;
    private static boolean skipping = true;
    private static int minBlockSize = 512;

    //Query parameters

    /* TRUE: Conjunctive Query FALSE: Disjunctive Query */
    private static boolean conjunctive = true;

    /* TRUE: MaxScore  FALSE: DAAT */
    private static boolean maxScore = true;

    /* TRUE: BM25  FALSE: TFIDF */
    private static boolean scoreMode = false;

    private static int numDocs = 5;

    /* SETTER AND GETTER SECTION */

    public static void setCompression(boolean compression) {
        Flags.compression = compression;
    }

    public static void setMaxScore(boolean maxScore) {
        Flags.maxScore = maxScore;
    }

    public static void setConjunctive(boolean conjunctive) {Flags.conjunctive = conjunctive;    }

    public static void setScoreMode(boolean scoreMode) { Flags.scoreMode = scoreMode;}

    public static void setSkipping(boolean skipping) { Flags.skipping = skipping; }

    public static void setNumDocs(int numDocs) {
        Flags.numDocs = numDocs;
    }

    public static void setMinBlockSize(int minBlockSize) {
        Flags.minBlockSize = minBlockSize;
    }

    public static int getNumDocs() {
        return numDocs;
    }

    public static int getMinBlockSize() {
        return minBlockSize;
    }

    public static boolean isCompression() {
        return compression;
    }

    public static boolean isScoreMode() {return scoreMode;}

    public static boolean isMaxScore() {
        return maxScore;
    }

    public static boolean isConjunctive() {return conjunctive;}

    public static boolean isSkipping() {return skipping;}

    public static void printOption(){
        System.out.println("DEFAULT OPTIONS:");
        System.out.println("--------------------------------------------");
        System.out.printf("%-22s %4s %s\n", "QueryMode", " -> ", isConjunctive()?"Conjunctive":"Disjunctive");
        if(!isConjunctive())
            System.out.printf("%-22s %4s %s\n", "Algorithm", " -> ", isMaxScore()?"MaxScore":"DAAT");
        System.out.printf("%-22s %4s %s\n", "Score", " -> ", isScoreMode()?"BM25":"TFIDF");
        System.out.printf("%-22s %4s %s\n", "SkippingMode", " -> ", isSkipping()?"True":"False");
        System.out.printf("%-22s %4s %d\n", "Documents to retrieve", " -> ", getNumDocs());
        System.out.println("--------------------------------------------");
    }

}
