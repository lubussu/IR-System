package it.unipi.mircv.bean;

import it.unipi.mircv.utils.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class SkipList {

    private String term;

    private ArrayList<SkipElem> skipList;

    public SkipList(){
        this.term = "";
        this.skipList = new ArrayList<>();
    }

    public SkipList(String term){
        this.term = term;
        this.skipList = new ArrayList<>();
    }

    public SkipList(String term, ArrayList<SkipElem> skipList){
        this.term = term;
        this.skipList = skipList;
    }

    public void addSkipElem(SkipElem se){
        this.skipList.add(se);
    }

    public boolean FromBinFile(FileChannel channel) throws IOException {
        String current_term = IOUtils.readTerm(channel);
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel);
        }
        return true;
    }

    public void updateFromBinFile(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();
        int sl_size = buffer.getInt();

        buffer = ByteBuffer.allocate(sl_size * 12);
        channel.read(buffer);
        buffer.flip();

        for (int j = 0; j < sl_size; j++) {
            int max_docid = buffer.getInt();
            long block_start = buffer.getLong();
            SkipElem elem = new SkipElem(max_docid, block_start);
            this.skipList.add(elem);
        }
        buffer.clear();
    }

    public void ToBinFile(FileChannel channel) {
        try {
            IOUtils.writeTerm(channel, term, skipList.size(), false);
            ByteBuffer skip_buffer = ByteBuffer.allocate(12);

            for(SkipElem elem : this.skipList){
                skip_buffer.putInt(elem.getMaxDocId());
                skip_buffer.putLong(elem.getBlockStartingOffset());
                skip_buffer.flip();
                // Write the buffer to the file
                channel.write(skip_buffer);
                skip_buffer.clear();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setSkipList(ArrayList<SkipElem> skipList) {
        this.skipList = skipList;
    }

    public String getTerm(){ return this.term; }

    public ArrayList<SkipElem> getSkipList(){ return this.skipList; }
}
