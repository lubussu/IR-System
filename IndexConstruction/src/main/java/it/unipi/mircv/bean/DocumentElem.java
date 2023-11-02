package it.unipi.mircv.bean;

import it.unipi.mircv.utils.IOUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class DocumentElem {
    private String docNo;
    private int docId;
    private int length;

    public DocumentElem() {
        this(0, "", 0);
    }

    public DocumentElem(int docId, String docNo, int length) {
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

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void ToBinFile(FileChannel channel) {
        try{
            byte[] descBytes = String.valueOf(this.docNo).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 8);

            // Populate the buffer for docNo
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(this.docId);
            buffer.putInt(this.length);
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean FromBinFile(FileChannel channel)  {
        try {
            this.docNo = IOUtils.readTerm(channel);
            if (this.docNo==null) {
                return false;
            } else {
                ByteBuffer buffer = ByteBuffer.allocate(8);
                channel.read(buffer);
                buffer.flip();
                this.docId = buffer.getInt();
                this.length = buffer.getInt();
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
