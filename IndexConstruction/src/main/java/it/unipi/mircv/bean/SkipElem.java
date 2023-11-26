package it.unipi.mircv.bean;

public class SkipElem {

    private int maxDocId;

    private long blockStartingOffset;

    public SkipElem() {
        this.maxDocId = 0;
        this.blockStartingOffset = 0;
    }

    public SkipElem(int maxDocId) {
        this.maxDocId = maxDocId;
        this.blockStartingOffset = 0;
    }

    public SkipElem(int maxDocId, long blockStartingOffset, int blockByteLength) {
        this.maxDocId = 0;
        this.blockStartingOffset = 0;
    }

    public int getMaxDocId() { return maxDocId; }

    public long getBlockStartingOffset() { return blockStartingOffset; }


    public void setMaxDocId(int maxDocId) {
        this.maxDocId = maxDocId;
    }

    public void setBlockStartingOffset(long blockStartingOffset) { this.blockStartingOffset = blockStartingOffset; }


}