package it.unipi.mircv.bean;

import it.unipi.mircv.compression.Unary;
import it.unipi.mircv.compression.VariableByte;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
        int pl_size = buffer.getInt(); // dimensione della posting_list salvata sul blocco
        if (Flags.isCompression()) {
            readCompressedPL(channel, pl_size);
        } else {
            readPL(channel, pl_size);
        }
        buffer.clear();
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

    public void ToBinFile(FileChannel channel, boolean skipping) {
        try {
            IOUtils.writeTerm(channel, term, pl.size(), true);

            if (Flags.isCompression()) {
                writeCompressedPL(channel, skipping);
            } else {
                writePL(channel, skipping);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeCompressedPL(FileChannel channel, boolean skipping) {
        try (FileChannel skipChannel = skipping ?
                FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo.bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND) : null)
        {
            ArrayList<Integer> docids = new ArrayList<>();
            ArrayList<Integer> freqs = new ArrayList<>();
            SkipList sl = new SkipList(this.term);
            ByteBuffer buffer;
            int numBytes;

            for (Posting p : this.pl) {
                docids.add(p.getDocId());
                freqs.add(p.getTermFreq());

                if (skipping && skipChannel != null && docids.size() != 1 && docids.size() % Math.floor(Math.sqrt(this.pl.size())) == 0) {
                    byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                    byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
                    numBytes = freqsCompressed.length + docsCompressed.length;

                    buffer = ByteBuffer.allocate(4 + numBytes);

                    buffer.putInt(numBytes);
                    buffer.put(docsCompressed);
                    buffer.put(freqsCompressed);

                    // Write the buffer to the file
                    buffer.flip();
                    channel.write(buffer);

                    SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position());
                    sl.addSkipElem(se);

                    docids.clear();
                    freqs.clear();
                }
            }
            byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
            byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
            numBytes = freqsCompressed.length + docsCompressed.length;

            buffer = ByteBuffer.allocate(4 + numBytes);

            buffer.putInt(numBytes);
            buffer.put(docsCompressed);
            buffer.put(freqsCompressed);

            // Write the buffer to the file
            buffer.flip();
            channel.write(buffer);

            if (skipping && skipChannel != null && docids.size() != 0) {
                SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position());
                sl.addSkipElem(se);
                sl.ToBinFile(skipChannel);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writePL(FileChannel channel, boolean skipping) {
        try(FileChannel skipChannel = skipping?
                FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo.bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND):null)
        {
            SkipList sl = new SkipList(this.term);
            int size;
            int post_count = 0;
            ByteBuffer buffer;
            if (skipping) {
                size = (int) Math.floor(Math.sqrt(pl.size()));
                buffer = ByteBuffer.allocate(4 + size * 8);
            } else {
                size = this.pl.size();
                buffer = ByteBuffer.allocate(size * 8);
            }
            int offset = (size - 1) * 4;
            for (Posting post : this.pl) {
                if (skipping) {
                    buffer.putInt(size);
                }
                buffer.putInt(post.getDocId());
                int current_position = buffer.position();
                buffer.putInt(current_position + offset, post.getTermFreq());
                buffer.position(current_position);
                post_count++;
                if (skipping && skipChannel != null && post_count == size) {
                    SkipElem se = new SkipElem(post.getDocId(), channel.position());
                    sl.addSkipElem(se);

                    post_count = 0;

                    // Write the buffer to the file
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
            }

            if (skipping) {
                buffer.putInt(post_count);
                SkipElem se = new SkipElem(this.pl.get(this.pl.size() - 1).getDocId(), channel.position());
                sl.addSkipElem(se);
                sl.ToBinFile(skipChannel);
            }

            // Write the buffer to the file
            buffer.flip();
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
