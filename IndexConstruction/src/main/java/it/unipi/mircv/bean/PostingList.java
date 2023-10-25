package it.unipi.mircv.bean;

import it.unipi.mircv.IndexConstruction;
import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.Utils.IOUtils;
import it.unipi.mircv.compression.Unary;
import it.unipi.mircv.compression.VariableByte;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class PostingList {
    private String term;
    private final ArrayList<Posting> pl;

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public ArrayList<Posting> getPl() {
        return pl;
    }

    public PostingList(String term) {
        this.term = term;
        this.pl = new ArrayList<>();
    }

    public PostingList(String term, Posting posting) {
        this(term);
        this.pl.add(posting);
    }

    public void addPosting(Posting p){
        this.pl.add(p);
    }

    public boolean FromBinFile(FileChannel channel, boolean compressed) throws IOException {
        String current_term = IOUtils.readTerm(channel);
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel,compressed);
        }
        return true;
    }

    public void ToBinFile(FileChannel channel, boolean compression) {
        try{
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4);
            long start_position = channel.position();
            DictionaryElem dict = InvertedIndex.getDictionary().get(this.term);
            dict.setOffset_block(start_position);

            // Populate the buffer for termLenght + term
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(pl.size());
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);

            if (compression) {
                ArrayList<Integer> docids = new ArrayList<>();
                ArrayList<Integer> freqs = new ArrayList<>();

                for (Posting p : this.pl) {
                    docids.add(p.getDocId());
                    freqs.add(p.getTermFreq());
                }

                byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);

                buffer = ByteBuffer.allocate( 4+docsCompressed.length + freqsCompressed.length);

                buffer.putInt(docsCompressed.length + freqsCompressed.length);
                buffer.put(docsCompressed);
                buffer.put(freqsCompressed);

            } else {
                buffer = ByteBuffer.allocate(pl.size() * 8);
                int offset = (pl.size()-1)*4;
                for (Posting post : this.pl) {
                    buffer.putInt(post.getDocId());
                    int current_position = buffer.position();
                    buffer.putInt(current_position + offset, post.getTermFreq());
                    buffer.position(current_position);
                }
            }

            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readCompressedPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(4);
        channel.read(buffer_pl);
        buffer_pl.flip();
        int byteRead = buffer_pl.getInt();
        if(byteRead < 0)
            return;
        buffer_pl = ByteBuffer.allocate(byteRead);
        channel.read(buffer_pl);
        buffer_pl.flip();

        byte[] bytes = buffer_pl.array();

        ArrayList<Integer> docids = VariableByte.fromVariableBytesToIntegers(bytes,pl_size);
        int starting_unary = docids.remove(0);
        bytes = Arrays.copyOfRange(bytes, starting_unary, bytes.length);
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(bytes);

        for (int j = 0; j < pl_size; j++) {
            int docid = docids.get(j);
            int freq = freqs.get(j);
            Posting post = new Posting(docid, freq);
            this.addPosting(post);
        }

        buffer_pl.clear();
    }

    public void readPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(pl_size * 4);
        channel.read(buffer_pl);
        buffer_pl.flip();

        int current_size = this.getPl().size();

        for (int j = 0; j < pl_size; j++) {
            int docid = buffer_pl.getInt();
            Posting post = new Posting(docid, 0);
            this.addPosting(post);
        }

        buffer_pl.clear();

        for (int j = 0; j < pl_size; j++) {
            int freq = buffer_pl.getInt();
            this.getPl().get(j + current_size).setTermFreq(freq);
        }
        buffer_pl.clear();
    }

    public void ToTextFile(String filename) {
        try (FileWriter fileWriter = new FileWriter(filename, true);
             PrintWriter writer = new PrintWriter(fileWriter)) {
            writer.write(this.term);
            for (Posting posting : this.pl) {
                writer.write(" " + posting.getDocId());
                writer.write(":" + posting.getTermFreq());
            }
            writer.write("\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printPostingList() {
        System.out.printf("Posting List of %s:\n", this.term);
        for (Posting p : this.getPl()) {
            System.out.printf("Docid: %d - Freq: %d\n", p.getDocId(), p.getTermFreq());
        }
    }

    public void updateFromBinFile(FileChannel channel, boolean compressed) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();
        int pl_size = buffer.getInt(); // dimensione della posting_list salvata sul blocco
        if (compressed) {
            readCompressedPL(channel, pl_size);
        } else {
            readPL(channel, pl_size);
        }
        buffer.clear();
    }

    public int compareTo(PostingList pl) {
        if(this.pl.size() > pl.pl.size()){
            return 1;
        }else{
            return -1;
        }
    }

}
