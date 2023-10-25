package it.unipi.mircv.bean;

import it.unipi.mircv.Utils.IOUtils;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class DictionaryElem {

    private String term;

    /* Number of documents in which the term appears at least once */
    private int df;

    /* Number of times the term appears in the collection */
    private int cf;

    /* Pointer to the beginning of posting list of term t */
    private int offset_posting_lists;

    /* Number of block in which the PL is stored */
    private int block_number;

    /* Pointer to the posting list on block */
    private long offset_block;

    /* Offset of the term frequencies posting list */
    private long offset_tf;

    /* Length of the term frequencies posting list */
    private int tf_len;

    /* Maximum term frequency of the term */
    private int maxTf;

    /* Offset of the skipping information */
    private int offset_skipInfo;

    /* Length of the skipping information */
    private int skipInfo_len;

    /* Inverse document frequencies */
    private double idf;

    /* Term upper bound for TF-IDF */
    private double maxTFIDF;

    /* Term upper bound for BM25 */
    private double maxBM25;

    public DictionaryElem(String term) {
        this(term, 0, 0);
    }

    public DictionaryElem(String term, int df, int cf) {
        this.term = term;
        this.df = df;
        this.cf = cf;
        this.offset_posting_lists = 0;
        this.block_number = 0;
        this.offset_block = 0;
        this.offset_tf = 0;
        this.tf_len = 0;
        this.maxTf = 0;
        this.offset_skipInfo = 0;
        this.skipInfo_len = 0;
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
    }

    public boolean FromBinFile(FileChannel channel) throws IOException {
        String current_term = IOUtils.readTerm(channel);
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel);
        }
        return true;
    }

    public void ToBinFile(FileChannel channel){
        try{
            byte[] descBytes = String.valueOf(this.term).getBytes(StandardCharsets.UTF_8);;
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 16);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(this.df);
            buffer.putInt(this.cf);
            buffer.putLong(this.offset_block);
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ToTextFile(String filename) {
        try (FileWriter fileWriter = new FileWriter(filename, true);
             PrintWriter writer = new PrintWriter(fileWriter)) {
            writer.write(this.term);
            writer.write(" " + this.df);
            writer.write(":" + this.cf);
            writer.write("\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateFromBinFile(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        channel.read(buffer);
        buffer.flip();
        int df = buffer.getInt();
        int cf = buffer.getInt();
        this.setDf(this.getDf() + df);
        this.setCf(this.getCf() + cf);
        this.offset_block = buffer.getLong();
        buffer.clear();
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public int getCf() {
        return cf;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public int getOffset_posting_lists() {
        return offset_posting_lists;
    }

    public void setOffset_posting_lists(int offset_posting_lists) {
        this.offset_posting_lists = offset_posting_lists;
    }

    public int getBlock_number() {
        return block_number;
    }

    public void setBlock_number(int block_number) {
        this.block_number = block_number;
    }

    public long getOffset_block() {
        return offset_block;
    }

    public void setOffset_block(long offset_block) {
        this.offset_block = offset_block;
    }

    public long getOffset_tf() {
        return offset_tf;
    }

    public void setOffset_tf(long offset_tf) {
        this.offset_tf = offset_tf;
    }

    public int getTf_len() {
        return tf_len;
    }

    public void setTf_len(int tf_len) {
        this.tf_len = tf_len;
    }

    public int getMaxTf() {
        return maxTf;
    }

    public void setMaxTf(int maxTf) {
        this.maxTf = maxTf;
    }

    public int getOffset_skipInfo() {
        return offset_skipInfo;
    }

    public void setOffset_skipInfo(int offset_skipInfo) {
        this.offset_skipInfo = offset_skipInfo;
    }

    public int getSkipInfo_len() {
        return skipInfo_len;
    }

    public void setSkipInfo_len(int skipInfo_len) {
        this.skipInfo_len = skipInfo_len;
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

    public int compareTo(DictionaryElem de) {
        if(this.getDf() > de.getDf()){
            return 1;
        }else{
            return -1;
        }
    }
}