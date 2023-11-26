package it.unipi.mircv.bean;

public class SkipElem {

    private int maxDocId;

    private long blockStartingOffset;

    private int numPostings;

    public SkipElem() {
        this.maxDocId = 0;
        this.blockStartingOffset = 0;
        this.numPostings = 0;
    }

    public SkipElem(int maxDocId) {
        this.maxDocId = maxDocId;
        this.blockStartingOffset = 0;
        this.numPostings = 0;
    }

    public SkipElem(int maxDocId, long blockStartingOffset, int numPostings) {
        this.maxDocId = maxDocId;
        this.blockStartingOffset = blockStartingOffset;
        this.numPostings = numPostings;
    }

    public int getMaxDocId() { return maxDocId; }

    public long getBlockStartingOffset() { return blockStartingOffset; }

    public int getNumPostings() { return numPostings; }

    public void setMaxDocId(int maxDocId) {
        this.maxDocId = maxDocId;
    }

    public void setBlockStartingOffset(long blockStartingOffset) { this.blockStartingOffset = blockStartingOffset; }

    public void setNumPostings(int numPostings) {
        this.numPostings = numPostings;
    }


}