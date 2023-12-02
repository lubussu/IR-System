package it.unipi.mircv.bean;

public class SkipElem {

    /* Maximum DocId of the block */
    private int maxDocId;

    /* Starting offset of the block in the PostingList */
    private int skipListOffset;

    /* Starting offset of the skipElem in the SkipInfo file */
    private long blockStartingOffset;

    private int block_size;


    public SkipElem() {
        this(0, 0, 0, 0);
    }

    public SkipElem(int maxDocId) {
        this(maxDocId, 0, 0, 0);
    }

    public SkipElem(int maxDocId, int skipListOffset, long blockStartingOffset, int block_size) {
        this.maxDocId = maxDocId;
        this.skipListOffset = skipListOffset;
        this.blockStartingOffset = blockStartingOffset;
        this.block_size = block_size;
    }

    public int getMaxDocId() { return maxDocId; }

    public long getBlockStartingOffset() { return blockStartingOffset; }

    public int getSkipListOffset() {
        return skipListOffset;
    }

    public int getBlock_size() {
        return block_size;
    }

    public void setSkipListOffset(int skipListOffset) {
        this.skipListOffset = skipListOffset;
    }

    public void setMaxDocId(int maxDocId) {
        this.maxDocId = maxDocId;
    }

    public void setBlockStartingOffset(long blockStartingOffset) { this.blockStartingOffset = blockStartingOffset; }

    public void setBlock_size(int blockSize) {
        this.block_size = blockSize;
    }

}