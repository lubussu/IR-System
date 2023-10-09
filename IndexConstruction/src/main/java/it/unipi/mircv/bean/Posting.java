package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Posting {
    private int docId;
    private int termFreq;

    public Posting(int docId, int termFreq) {
        this.docId = docId;
        this.termFreq = termFreq;
    }
}
