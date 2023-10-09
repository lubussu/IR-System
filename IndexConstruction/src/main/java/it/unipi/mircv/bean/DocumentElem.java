package it.unipi.mircv.bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DocumentElem {
    private String docNo;
    private long docId;
    private long length;

    public DocumentElem() {
        this(0, "", 0);
    }

    public DocumentElem(long docId, String docNo, long length) {
        this.docId = docId;
        this.docNo = docNo;
        this.length = length;
    }
}
