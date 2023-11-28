package it.unipi.mircv.bean;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.compression.Unary;
import it.unipi.mircv.compression.VariableByte;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;

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

    public Iterator<Posting> postingIterator;

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
            IOUtils.writeTerm(channel, term, pl.size());
            if(Flags.isSkipping() && skipChannel != null){
                int size = (int) Math.ceil(Math.sqrt(this.pl.size()));
                IOUtils.writeTerm(skipChannel, term, size);
            }

            if (compression) {
                writeCompressedPL(channel, skipChannel);
            } else {
                writePL(channel, skipChannel);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readCompressedPL(FileChannel channel, int pl_size) throws IOException {
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

    private void readPL(FileChannel channel, int pl_size) throws IOException {
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

    private void writeCompressedBlock(FileChannel channel, ArrayList<Integer> docids, ArrayList<Integer> freqs) throws IOException {
        byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
        byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);

        ByteBuffer buffer = ByteBuffer.allocate(4 + docsCompressed.length + freqsCompressed.length);

        buffer.putInt(docsCompressed.length + freqsCompressed.length);
        buffer.put(docsCompressed);
        buffer.put(freqsCompressed);

        // Write the buffer to the file
        buffer.flip();
        channel.write(buffer);
    }

    private void writeCompressedPL(FileChannel channel, FileChannel skipChannel) throws IOException {
        ArrayList<Integer> docids = new ArrayList<>();
        ArrayList<Integer> freqs = new ArrayList<>();

        for (Posting p : this.pl) {
            docids.add(p.getDocId());
            freqs.add(p.getTermFreq());

            if(Flags.isSkipping() && skipChannel != null && this.pl.size() % Math.round(Math.sqrt(docids.size())) == 0) {
                writeCompressedBlock(channel, docids, freqs);

                SkipElem se = new SkipElem(docids.get(docids.size()-1), channel.position(), docids.size());
                se.ToBinFile(skipChannel);

                docids.clear();
                freqs.clear();
            }
        }
        writeCompressedBlock(channel, docids, freqs);

        if(Flags.isSkipping() && skipChannel != null){
            SkipElem se = new SkipElem(docids.get(docids.size()-1), channel.position(), docids.size());
            se.ToBinFile(skipChannel);
        }
    }

    private void writePL(FileChannel channel, FileChannel skipChannel) throws IOException {
        int block_dim = (int) Math.round(Math.sqrt(pl.size()));
        int post_count = 0;
        ByteBuffer buffer = ByteBuffer.allocate(block_dim * 8);
        int offset = (block_dim-1)*4;
        for (Posting post : this.pl) {
            buffer.putInt(post.getDocId());
            int current_position = buffer.position();
            buffer.putInt(current_position + offset, post.getTermFreq());
            buffer.position(current_position);
            post_count++;
            if(Flags.isSkipping() && skipChannel != null && post_count == block_dim){
                SkipElem se = new SkipElem(post.getDocId(), channel.position(), post_count);
                se.ToBinFile(skipChannel);
                post_count = 0;

                // Write the buffer to the file
                buffer.flip();
                channel.write(buffer);
                buffer.clear();
            }
        }

        if(Flags.isSkipping() && skipChannel != null && post_count == block_dim) {
            SkipElem se = new SkipElem(this.pl.get(this.pl.size()-1).getDocId(), channel.position(), post_count);
            se.ToBinFile(skipChannel);
        }
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
