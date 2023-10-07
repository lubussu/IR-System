package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
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

    public void addPosting(Posting p) {
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

    public void ToBinFile(String filename){
        try (FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);;
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4 + pl.size() * 8);
            // Populate the buffer
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(pl.size());
            for (Posting post : pl) {
                buffer.putInt(post.getDocId());
            }
            for (Posting post : pl) {
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
