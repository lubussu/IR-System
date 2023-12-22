package it.unipi.mircv.bean;

import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.utils.Scorer;
import it.unipi.mircv.utils.Flags;

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

    /* Inverse document frequencies */
    private double idf;

    /* Number of block in which the PL is stored */
    private int block_number;

    /* Pointer to the beginning of posting list of term t in cache*/
    private int offset_posting_lists;

    /* Pointer to the posting list on block (in the file)*/
    private long offset_block_pl;

    /* Pointer to the beginning of skipping list of term t in memory*/
    private int offset_skip_lists;

    /* Pointer to the skipping list (in the file)*/
    private long offset_block_sl;

    /* Maximum term frequency of the term */
    private int maxTf;

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
        this.idf = 0;
        this.offset_posting_lists = 0;
        this.block_number = 0;
        this.offset_block_pl = 0;
        this.maxTf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
    }

    /**
     * Compute the MaxBM25 score for the term of the posting list.
     *
     * @param pl Channel to the file to read
     */
    public void computeMaxBM25(PostingList pl) {
        for (Posting p: pl.getPl()) {
            double current_BM25 = Scorer.scoreDocument(p, this.getIdf(), Flags.isScoreMode());

            if (current_BM25 > this.getMaxBM25())
                this.setMaxBM25(current_BM25);
        }
    }

    /**
     * Compute the maximum term frequency for the term of the posting list.
     *
     * @param list Channel to the file to read
     */
    public void computeMaxTf(PostingList list) {
        for (Posting posting : list.getPl()) {
            if (posting.getTermFreq() > this.maxTf)
                this.maxTf = posting.getTermFreq();
        }
    }

    /**
     * Compute the TFIDF score for the term of the posting list.
     */
    public void computeMaxTFIDF() {
        this.maxTFIDF = (1 + Math.log10(this.maxTf)) * this.idf;
    }

    /**
     * Read the object from a file in binary code.
     *
     * @param channel Channel to the file to read
     * @throws IOException Error while opening the file channel
     */
    public boolean FromBinFile(FileChannel channel) throws IOException {
        String current_term = IOUtils.readTerm(channel);
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel);
        }
        return true;
    }

    /**
     * Write the object to a file in binary code.
     *
     * @param channel Channel to the file to write
     * @throws IOException Error while opening the file channel
     */
    public void ToBinFile(FileChannel channel){
        try{
            byte[] descBytes = String.valueOf(this.term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 48);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(this.df);
            buffer.putInt(this.cf);
            buffer.putInt(this.block_number);
            buffer.putLong(this.offset_block_pl);
            buffer.putInt(this.maxTf);
            buffer.putDouble(this.idf);
            buffer.putDouble(this.maxTFIDF);
            buffer.putDouble(this.maxBM25);

            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write the object to a file in text format.
     *
     * @param filename Path of the file to write
     * @throws IOException Error while opening the file channel
     */
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

    /**
     * Read the object parameters from a file in binary code.
     *
     * @param channel Channel to the file to read
     * @throws IOException Error while opening the file channel
     */
    public void updateFromBinFile(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(48);
        channel.read(buffer);
        buffer.flip();
        int df = buffer.getInt();
        int cf = buffer.getInt();
        this.setDf(this.getDf() + df);
        this.setCf(this.getCf() + cf);
        this.block_number = buffer.getInt();
        this.offset_block_pl = buffer.getLong();
        this.maxTf = buffer.getInt();
        this.idf = buffer.getDouble();
        this.maxTFIDF = buffer.getDouble();
        this.maxBM25 = buffer.getDouble();
        buffer.clear();
    }

    public String getTerm() {
        return term;
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

    public long getOffset_block_pl() {
        return offset_block_pl;
    }

    public void setOffset_block_pl(long offset_block_pl) {
        this.offset_block_pl = offset_block_pl;
    }

    public int getMaxTf() {
        return maxTf;
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

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

    public int getOffset_skip_lists(){ return offset_skip_lists; }

    public void setOffset_skip_lists(int offset_skip_lists){ this.offset_skip_lists = offset_skip_lists; }

    public long getOffset_block_sl(){ return this.offset_block_sl; }

    public void setOffset_block_sl(long offset_block_sl){ this.offset_block_sl = offset_block_sl; }

    /**
     * Comparator of two Dictionary elements.
     *
     * @param de The element to compare.
     */
    public int compareTo(DictionaryElem de) {
        if(this.getDf() > de.getDf()){
            return 1;
        }else if(this.getDf() < de.getDf()){
            return -1;
        }else{
            return 0;
        }
    }
}