package it.unipi.mircv.utils;

public class DocumentScore implements Comparable<DocumentScore> {
    private final int docId;
    private final double score;

    public DocumentScore(int docId, double score) {
        this.docId = docId;
        this.score = score;
    }

    @Override
    public int compareTo(DocumentScore other) {
        return Double.compare(other.score, this.score);
    }

    public int getDocId() {
        return docId;
    }

    public double getScore() {
        return score;
    }
}