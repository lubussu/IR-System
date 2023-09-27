package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        pl.add(p);
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
}
