package it.unipi.mircv.algorithm;

import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.Posting;

public class Scorer {

    private static final double k1 = 1.5;
    private static final double b = 0.75;

    public static double scoreDocument(Posting posting, double idf, String scoringFunction) {
        return ((scoringFunction.equals("bm25")) ? computeBM25(posting, idf) : computeTFIDF(posting, idf));
    }

    private static double computeBM25(Posting posting, double idf) {
        double tf = (1 + Math.log10(posting.getTermFreq()));
        int docLen = 0; //inserire docLen
        double avgDocLen = (double) CollectionInfo.getCollection_total_len() / CollectionInfo.getCollection_size();

        return idf * tf / (tf + k1 * (1 - b + b * docLen / avgDocLen));
    }

    private static double computeTFIDF(Posting posting, double idf) {
        return idf * (1 + Math.log10(posting.getTermFreq()));
    }
}
