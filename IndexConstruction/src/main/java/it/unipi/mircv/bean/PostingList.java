package it.unipi.mircv.bean;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.compression.Unary;
import it.unipi.mircv.compression.VariableByte;
import it.unipi.mircv.utils.Flags;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class PostingList {

    private String term;

    private ArrayList<Posting> pl;

    public Iterator<Posting> postingIterator = null;

    private Posting actualPosting;

    public PostingList(String term) {
        this.term = term;
        this.pl = new ArrayList<>();
        this.actualPosting = null;
        this.postingIterator = pl.iterator();
    }

    public PostingList(String term, ArrayList<Posting> list) {
        this.term = term;
        this.pl = list;
        this.postingIterator = pl.iterator();
        assert (!list.isEmpty());
        this.actualPosting = postingIterator.next();
    }

    public PostingList(String term, Posting posting) {
        this(term);
        this.pl.add(posting);
    }

    public void initList(){
        this.postingIterator = pl.iterator();
        assert (!pl.isEmpty());
        this.actualPosting = postingIterator.next();
    }

    public void addPosting(Posting p){
        this.pl.add(p);
    }

    public void next() {
        if (!postingIterator.hasNext()) {
            actualPosting = null;
        }else {
            actualPosting = postingIterator.next();
        }
    }

    public void nextGEQ(int docID) {
        while (postingIterator.hasNext() && actualPosting.getDocId() < docID)
            actualPosting = postingIterator.next();
    }

    public boolean FromBinFile(FileChannel channel, boolean compressed) throws IOException {
        String current_term = IOUtils.readTerm(channel);
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel,compressed);
        }
        return true;
    }

    public void ToBinFile(FileChannel channel, FileChannel skipChannel, boolean compression) {
        try{
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4);
            long start_position = channel.position();
            DictionaryElem dict = InvertedIndex.getDictionary().get(this.term);
            dict.setOffset_block(start_position);

            // Populate the buffer for termLenght + term
            buffer.putInt(descBytes.length);
            buffer.put(descBytes);
            buffer.putInt(pl.size());
            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);

            if(Flags.isSkipping && skipChannel != null){
                initSkipFile(skipChannel);
            }

            if (compression) {
                ArrayList<Integer> docids = new ArrayList<>();
                ArrayList<Integer> freqs = new ArrayList<>();

                for (Posting p : this.pl) {
                    docids.add(p.getDocId());
                    freqs.add(p.getTermFreq());

                    if(Flags.isSkipping && skipChannel != null && Math.round(Math.sqrt(docids.size())) % this.pl.size() == 0) {
                        byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                        byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);

                        buffer = ByteBuffer.allocate(4 + docsCompressed.length + freqsCompressed.length);

                        buffer.putInt(docsCompressed.length + freqsCompressed.length);
                        buffer.put(docsCompressed);
                        buffer.put(freqsCompressed);
                        buffer.flip();

                        skipBlockToBinFile(skipChannel, docids.getLast(), channel.position());

                        // Write the buffer to the file
                        channel.write(buffer);

                        docids.clear();
                        freqs.clear();
                    }
                }
                byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);

                buffer = ByteBuffer.allocate(4 + docsCompressed.length + freqsCompressed.length);

                buffer.putInt(docsCompressed.length + freqsCompressed.length);
                buffer.put(docsCompressed);
                buffer.put(freqsCompressed);

                if(Flags.isSkipping && skipChannel != null){
                    skipBlockToBinFile(skipChannel, docids.getLast(), channel.position());
                }

                buffer.flip();
                // Write the buffer to the file
                channel.write(buffer);
            } else {
                buffer = ByteBuffer.allocate(pl.size() * 8);
                int offset = (pl.size()-1)*4;
                for (Posting post : this.pl) {
                    buffer.putInt(post.getDocId());
                    int current_position = buffer.position();
                    buffer.putInt(current_position + offset, post.getTermFreq());
                    buffer.position(current_position);
                }
                buffer.flip();
                // Write the buffer to the file
                channel.write(buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readCompressedPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(4);
        channel.read(buffer_pl);
        buffer_pl.flip();
        int byteRead = buffer_pl.getInt();
        if(byteRead < 0)
            return;
        buffer_pl = ByteBuffer.allocate(byteRead);
        channel.read(buffer_pl);
        buffer_pl.flip();

        byte[] bytes = buffer_pl.array();

        ArrayList<Integer> docids = VariableByte.fromVariableBytesToIntegers(bytes,pl_size);
        int starting_unary = docids.remove(0);
        bytes = Arrays.copyOfRange(bytes, starting_unary, bytes.length);
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(bytes);

        for (int j = 0; j < pl_size; j++) {
            int docid = docids.get(j);
            int freq = freqs.get(j);
            Posting post = new Posting(docid, freq);
            this.addPosting(post);
        }

        initList();

        buffer_pl.clear();
    }

    public void readPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(pl_size * 4);
        channel.read(buffer_pl);
        buffer_pl.flip();

        int current_size = this.getPl().size();

        for (int j = 0; j < pl_size; j++) {
            int docid = buffer_pl.getInt();
            Posting post = new Posting(docid, 0);
            this.addPosting(post);
        }

        initList();

        buffer_pl.clear();

        for (int j = 0; j < pl_size; j++) {
            int freq = buffer_pl.getInt();
            this.getPl().get(j + current_size).setTermFreq(freq);
        }

        buffer_pl.clear();
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

    public void printPostingList() {
        System.out.printf("Posting List of %s:\n", this.term);
        for (Posting p : this.getPl()) {
            System.out.printf("(Docid: %d - Freq: %d)\t", p.getDocId(), p.getTermFreq());
        }
    }

    public void updateFromBinFile(FileChannel channel, boolean compressed) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();
        int pl_size = buffer.getInt(); // dimensione della posting_list salvata sul blocco
        if (compressed) {
            readCompressedPL(channel, pl_size);
        } else {
            readPL(channel, pl_size);
        }
        buffer.clear();
    }

    public void initSkipFile(FileChannel channel){
        try{
            byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
            ByteBuffer skip_buffer = ByteBuffer.allocate(4 + descBytes.length);
            long start_position = channel.position();
            DictionaryElem dict = InvertedIndex.getDictionary().get(this.term);
            dict.setOffset_block(start_position);

            // Populate the buffer for termLenght + term
            skip_buffer.putInt(descBytes.length);
            skip_buffer.put(descBytes);
            skip_buffer.flip();
            // Write the buffer to the file
            channel.write(skip_buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void skipBlockToBinFile(FileChannel channel, int skipId, long offset){
        try{
            ByteBuffer skip_buffer = ByteBuffer.allocate(4 + 8);

            // Populate the buffer for termLenght + term
            skip_buffer.putInt(skipId);
            skip_buffer.putLong(offset);
            skip_buffer.flip();
            // Write the buffer to the file
            channel.write(skip_buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Posting getActualPosting() { return actualPosting; }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public ArrayList<Posting> getPl() {
        return pl;
    }

}
