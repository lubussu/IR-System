package it.unipi.mircv.bean;

import it.unipi.mircv.InvertedIndex;
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
        int sl_size = buffer.getInt(); // dimensione della posting_list salvata sul blocco
        readSkipList(channel, sl_size);
        buffer.clear();
    }

    public void readSkipList(FileChannel channel, int sl_size) throws IOException {
        ByteBuffer buffer_sl = ByteBuffer.allocate(sl_size * 16);
        channel.read(buffer_sl);
        buffer_sl.flip();

        for (int j = 0; j < sl_size; j++) {
            int max_docid = buffer_sl.getInt();
            long block_start = buffer_sl.getLong();
            int post_num = buffer_sl.getInt();
            SkipElem elem = new SkipElem(max_docid, block_start, post_num);
            this.skipList.add(elem);
        }

        buffer_sl.clear();
    }

    public void ToBinFile(FileChannel channel) {
        try {
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer skip_buffer = ByteBuffer.allocate(4 + descBytes.length + 4);

            // Populate the buffer for termLenght + term
            skip_buffer.putInt(descBytes.length);
            skip_buffer.put(descBytes);
            skip_buffer.putInt(skipList.size());
            skip_buffer.flip();
            // Write the buffer to the file
            channel.write(skip_buffer);

            for(SkipElem elem : this.skipList){
                skip_buffer = ByteBuffer.allocate(16);
                skip_buffer.putInt(elem.getMaxDocId());
                skip_buffer.putLong(elem.getBlockStartingOffset());
                skip_buffer.putInt(elem.getNumPostings());
                skip_buffer.flip();
                // Write the buffer to the file
                channel.write(skip_buffer);
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
