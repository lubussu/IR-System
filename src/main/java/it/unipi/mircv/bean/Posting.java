package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Posting {
    private final int docId;
    private final int termFreq;

    public Posting(int docId, int termFreq) {
        this.docId = docId;
        this.termFreq = termFreq;
    }
}
