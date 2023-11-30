package it.unipi.mircv.bean;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SkipElem {

    private int maxDocId;

    private long blockStartingOffset;

    public SkipElem() {
        this(0, 0);
    }

    public SkipElem(int maxDocId) {
        this(maxDocId, 0);
    }

    public SkipElem(int maxDocId, long blockStartingOffset) {
        this.maxDocId = maxDocId;
        this.blockStartingOffset = blockStartingOffset;
    }

    public int getMaxDocId() { return maxDocId; }

    public long getBlockStartingOffset() { return blockStartingOffset; }

    public void setMaxDocId(int maxDocId) {
        this.maxDocId = maxDocId;
    }

    public void setBlockStartingOffset(long blockStartingOffset) { this.blockStartingOffset = blockStartingOffset; }

}