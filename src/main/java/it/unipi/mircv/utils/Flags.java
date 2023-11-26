package it.unipi.mircv.utils;

public class Flags {

    private static boolean compression = true;

    /* TRUE: MaxScore  FALSE: DAAT */
    private static boolean maxScore = true;

    /* TRUE: BM25  FALSE: TFIDF */
    private static boolean scoreMode = true;

    /* TRUE: Conjunctive Query  FALSE: Disjunctive Query */
    private static boolean queryMode = false;

    private static boolean skipping = true;


    /* SETTER AND GETTER SECTION */

    public static void setCompression(boolean compression) {
        Flags.compression = compression;
    }

    public static void setMaxScore(boolean maxScore) {
        Flags.maxScore = maxScore;
    }

    public static void setQueryMode(boolean queryMode) {Flags.queryMode = queryMode;    }

    public static void setScoreMode(boolean scoreMode) { Flags.scoreMode = scoreMode;}

    public static boolean isCompression() {
        return !compression;
    }

    public static boolean isScoreMode() {return scoreMode;}

    public static boolean isMaxScore() {
        return maxScore;
    }

    public static boolean isQueryMode() {return queryMode;}

    public static boolean isSkipping() {return skipping;}
}
