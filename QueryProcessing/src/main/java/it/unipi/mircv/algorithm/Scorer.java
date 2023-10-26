package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;

public class Scorer {

    private static final double k1 = 1.5;
    private static final double b = 0.75;

    public static double scoreDocument(Posting posting, boolean scoreMode, double idf) {
        int docLen = InvertedIndex.getDocTable().get(posting.getDocId()).getLength();
        return (scoreMode) ? computeBM25(posting, idf, docLen) : computeTFIDF(posting, idf);
    }

    private static double computeBM25(Posting posting, double idf, int docLen) {
        int tf = posting.getTermFreq();
        double avgDocLen = (double) CollectionInfo.getCollection_total_len() / CollectionInfo.getCollection_size();
        return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen / avgDocLen));
    }

    private static double computeTFIDF(Posting posting, double idf) {
        return idf * (1 + Math.log10(posting.getTermFreq()));
    }
}
