package it.unipi.mircv.bean;

public class Posting {
    private int docId;
    private int termFreq;

    public Posting(int docId, int termFreq) {
        this.docId = docId;
        this.termFreq = termFreq;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getTermFreq() {
        return termFreq;
    }

    public void setTermFreq(int termFreq) {
        this.termFreq = termFreq;
    }
}
