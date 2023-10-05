package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Getter
@Setter
public class DictionaryElem {

    private String term;

    /* Number of documents in which the term appears at least once */
    private int df;

    /* Number of times the term appears in the collection */
    private int cf;

    /* Offset of the docIDs posting list */
    private long offset_docids;

    /* Length of the docIDs posting list */
    private int docids_len;

    /* Offset of the term frequencies posting list */
    private long offset_tf;

    /* Length of the term frequencies posting list */
    private int tf_len;

    /* Maximum term frequency of the term */
    private int maxTf;

    /* Offset of the skipping information */
    private long offset_skipInfo;

    /* Length of the skipping information */
    private int skipInfo_len;

    /* Inverse document frequencies */
    private double idf;

    /* Term upper bound for TF-IDF */
    private double maxTFIDF;

    /* Term upper bound for BM25 */
    private double maxBM25;

    public DictionaryElem() {
        this("", 0, 0);
    }

    public DictionaryElem(String term, int df, int cf) {
        this.term = term;
        this.df = df;
        this.cf = cf;
        this.offset_docids = 0;
        this.docids_len = 0;
        this.offset_tf = 0;
        this.tf_len = 0;
        this.maxTf = 0;
        this.offset_skipInfo = 0;
        this.skipInfo_len = 0;
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
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

    public void ToBinFile(String filename){
        try (FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            byte[] descBytes = String.valueOf(this.term).getBytes(StandardCharsets.UTF_8);;
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 8);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(this.df);
            buffer.putInt(this.cf);
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}