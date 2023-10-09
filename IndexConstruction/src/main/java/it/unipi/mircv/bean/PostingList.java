package it.unipi.mircv.bean;

import it.unipi.mircv.compression.Unary;
import it.unipi.mircv.compression.VariableByte;
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
import java.util.ArrayList;
import java.util.Arrays;

@Setter
@Getter
public class PostingList {
    private String term;
    private final ArrayList<Posting> pl;

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

    public void ToBinFile(String filename, boolean compression) {
        try (FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer_term = ByteBuffer.allocate(4 + descBytes.length + 4);
            
            // Populate the buffer_term
            buffer_term.putInt(descBytes.length);
            buffer_term.put(descBytes);
            buffer_term.putInt(pl.size());
            buffer_term.flip();
            // Write the buffer to the file
            channel.write(buffer_term);

            ByteBuffer buffer;
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
                for (Posting post : this.pl)
                    buffer.putInt(post.getDocId());

                for (Posting post : this.pl)
                    buffer.putInt(post.getTermFreq());
            }

            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean FromBinFile(FileChannel channel, boolean compressed) throws IOException {
        ByteBuffer buffer_size = ByteBuffer.allocate(4);
        int byteRead = channel.read(buffer_size);
        if(byteRead == -1)
            return false;

        buffer_size.flip();
        int termSize = buffer_size.getInt();
        ByteBuffer buffer_term = ByteBuffer.allocate(termSize);
        channel.read(buffer_term);
        buffer_term.flip();
        byte[] bytes = new byte[termSize];
        buffer_term.get(bytes);
        String current_term = new String(bytes, StandardCharsets.UTF_8);
        if (!current_term.equals(term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            buffer_size = ByteBuffer.allocate(4);
            channel.read(buffer_size);
            buffer_size.flip();
            int pl_size = buffer_size.getInt(); // dimensione della posting_list salvata sul blocco
            if (compressed) {
                readCompressedPL(channel, pl_size);
            } else {
                readPL(channel, pl_size);
            }
        }
        return true;
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

    public void readCompressedPL(FileChannel channel, int pl_size) throws IOException {
        //byte[] docids = VariableByte.fromVariableBytesToIntegers();
        ByteBuffer buffer_pl = ByteBuffer.allocate(4);
        channel.read(buffer_pl);
        buffer_pl.flip();
        int byteRead = buffer_pl.getInt();
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

    public void printPostingList() {
        System.out.printf("Posting List of %s:\n", this.term);
        for (Posting p : this.getPl()) {
            System.out.printf("Docid: %d - Freq: %d\n", p.getDocId(), p.getTermFreq());
        }
    }

}
