package it.unipi.mircv.utils;

import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.Posting;

public class Scorer {

    private static final double k1 = 1.5;
    private static final double b = 0.75;

    /**
     * Calls the scoring function to be executed.
     * @param posting Posting on which compute the score
     * @param idf Inverted document frequency of the term
     * @param scoreMode Type of score to compute on the posting (TFIDF or BM25)
     * @return The new score of the document
     */
    public static double scoreDocument(Posting posting, double idf, boolean scoreMode) {
        return (scoreMode) ? computeBM25(posting, idf) : computeTFIDF(posting, idf);
    }

    /**
     * Computes the BM25 score of a new posting for a document.
     * @param posting Posting on which compute the score
     * @param idf Inverted document frequency of the term
     * @return The new score of the document
     */
    private static double computeBM25(Posting posting, double idf) {
        double tf = (1 + Math.log10(posting.getTermFreq()));
        int docLen = 0;
        double avgDocLen = (double) CollectionInfo.getCollection_total_len() / CollectionInfo.getCollection_size();

        return idf * tf / (tf + k1 * (1 - b + b * docLen / avgDocLen));
    }

    /**
     * Computes the TFIDF score of a new posting for a document.
     * @param posting Posting on which compute the score
     * @param idf Inverted document frequency of the term
     * @return The new score of the document
     */
    private static double computeTFIDF(Posting posting, double idf) {
        return idf * (1 + Math.log10(posting.getTermFreq()));
    }
}
