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
/*
    public void ToBinFile(String filename){
        try (FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);;
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4 + pl.size() * 8);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(pl.size());

            for (Posting post : this.pl) {
                buffer.putInt(post.getDocId());
            }
            for (Posting post : this.pl) {
                buffer.putInt(post.getTermFreq());
            }
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void CompressedToBinFile(String filename){
        try (FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4 + pl.size() * 8);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(pl.size());

            ArrayList<Integer> docids = new ArrayList<>();
            ArrayList<Integer> freqs = new ArrayList<>();

            for (Posting p : this.pl) {
                docids.add(p.getDocId());
                freqs.add(p.getTermFreq());
            }
            byte[] freqsCompressed = Unary.fromIntToUnary(freqs);

            for (Integer docid : docids) {
                buffer.putInt(docid);
            }
            for (byte freq : freqsCompressed) {
                buffer.put(freq);
            }
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
*/
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

                buffer = ByteBuffer.allocate( docsCompressed.length + freqsCompressed.length);

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

}
