package it.unipi.mircv.bean;

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

    public String getDocNo() {
        return docNo;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public long getDocId() {
        return docId;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
