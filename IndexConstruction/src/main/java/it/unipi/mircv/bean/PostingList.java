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
        if(!pl.isEmpty())
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
        if(Flags.isSkipping() && (this.pl.isEmpty() || this.pl.get(this.pl.size()-1).getDocId() < docID)){
            SkipList sl = getSkipList();

            for(SkipElem se : sl.getSl()){
                if(se.getMaxDocId() >= docID){
                    int starting_point = this.pl.size();
                    readSkippingBlock(se);
                    postingIterator = this.pl.listIterator(starting_point);
                    if (postingIterator.hasNext()) {
                        actualPosting = postingIterator.next();
                    } else {
                        actualPosting = null;
                    }
                    break;
                }
            }
        }

        while (postingIterator.hasNext() && actualPosting.getDocId() < docID) {
            actualPosting = postingIterator.next();
        }
    }

    public boolean FromBinFile(FileChannel channel, boolean skipping) throws IOException {
        String current_term = IOUtils.readTerm(channel);

        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel, skipping);
        }
        return true;
    }

    public void updateFromBinFile(FileChannel channel, boolean skipping) throws IOException {
        int num_blocks = 1;
        SkipList sl = null;
        if(skipping){
            sl = getSkipList();
            num_blocks = sl.getSl().size();
        }
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();
        int pl_size = buffer.getInt(); // dimensione della posting_list salvata completa

        for(int i=0; i<num_blocks; i++){
            pl_size = skipping? sl.getSl().get(i).getBlock_size():pl_size;
            if (Flags.isCompression()) {
                readCompressedPL(channel, pl_size);
            } else{
                readPL(channel, pl_size);
            }
        }
    }

    public void readSkippingBlock(SkipElem se){
        int block_number = InvertedIndex.getDictionary().get(term).getBlock_number();
        String filename = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged"+block_number;
        FileChannel channel = IOUtils.getFileChannel(filename, "read");

        try {
            channel.position(se.getBlockStartingOffset());
            if (Flags.isCompression()) {
                readCompressedPL(channel, se.getBlock_size());
            } else{
                readPL(channel, se.getBlock_size());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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

        assert docids.size() == freqs.size();
        for (int j = 0; j < pl_size; j++) {
            int docid = docids.get(j);
            int freq = freqs.get(j);
            this.addPosting(new Posting(docid, freq));
        }

        initList();
        buffer_pl.clear();
    }

    private void readPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(pl_size * 4);
        channel.read(buffer_pl);
        buffer_pl.flip();

        for (int j = 0; j < pl_size; j++) {
            int docid = buffer_pl.getInt();
            int current_position = buffer_pl.position();
            int freq = buffer_pl.getInt(current_position + (pl_size - 1) * 4); //freq
            buffer_pl.position(current_position);

            this.addPosting(new Posting(docid, freq));
        }

        initList();
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

            int blockSize = (int) Math.floor(Math.sqrt(this.pl.size()));
            blockSize = blockSize >= Flags.getMinBlockSize()? blockSize : this.pl.size();

            for (Posting p : this.pl) {
                docids.add(p.getDocId());
                freqs.add(p.getTermFreq());

                if (skipping && skipChannel != null && docids.size() % blockSize == 0) {
                    byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                    byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
                    numBytes = freqsCompressed.length + docsCompressed.length;

                    //i is the startin position of the block inside the postingList
                    SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position(), docids.size());
                    sl.addSkipElem(se);

                    buffer = ByteBuffer.allocate(4 + numBytes);

                    buffer.putInt(numBytes);
                    buffer.put(docsCompressed);
                    buffer.put(freqsCompressed);

                    // Write the buffer to the file
                    buffer.flip();
                    channel.write(buffer);

                    docids.clear();
                    freqs.clear();
                }
            }
            if (docids.isEmpty()) {
                if (skipChannel != null) {
                    sl.ToBinFile(skipChannel);
                }
                return;
            }

            byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
            byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
            numBytes = freqsCompressed.length + docsCompressed.length;

            if (skipping && skipChannel != null) {
                SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position(), docids.size());
                sl.addSkipElem(se);
                sl.ToBinFile(skipChannel);
            }

            buffer = ByteBuffer.allocate(4 + numBytes);
            buffer.putInt(numBytes);
            buffer.put(docsCompressed);
            buffer.put(freqsCompressed);

            // Write the buffer to the file
            buffer.flip();
            channel.write(buffer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writePL(FileChannel channel, boolean skipping) {
        try(FileChannel skipChannel = skipping?
                FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo.bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND):null)
        {
            SkipList sl = new SkipList(this.term);
            int size = skipping ? (int) Math.floor(Math.sqrt(pl.size())) : this.pl.size();
            ByteBuffer buffer = skipping ? ByteBuffer.allocate(4 + size * 8) : ByteBuffer.allocate(size * 8);

            int post_count = 0;
            int offset = (size - 1) * 4; //freqs starting position in the buffer
            int offset_pl = 0;

            for (Posting post : this.pl) {
                if (skipping) {
                    buffer.putInt(size); //
                }
                buffer.putInt(post.getDocId()); //docId
                int current_position = buffer.position();
                buffer.putInt(current_position + offset, post.getTermFreq()); //freq
                buffer.position(current_position);

                if(post_count % size == 0){
                    offset_pl = post_count;
                }
                post_count++;
                if (skipping && post_count % size == 0) {
                    SkipElem se = new SkipElem(post.getDocId(), channel.position(), size);
                    sl.addSkipElem(se);

                    // Write the buffer to the file
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
            }
            if (post_count % size == 0) { //non ho un altro skipElem da aggiungere ma devo scrivere la skipList
                if (skipChannel != null) {
                    sl.ToBinFile(skipChannel);
                }
                return;
            } else if (skipping) {
                buffer.putInt(post_count-offset_pl-1);
                SkipElem se = new SkipElem(this.pl.get(this.pl.size() - 1).getDocId(), channel.position(), post_count%size);
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
                writer.write("\n");
            }
            writer.write("\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printPostingList() {
        System.out.printf("Posting List of %s:\n", this.term);
        for (Posting p : this.getPl()) {
            System.out.printf("(Docid: %d - Freq: %d)\n", p.getDocId(), p.getTermFreq());
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

    public SkipList getSkipList(){
        int offset_sl = InvertedIndex.getDictionary().get(this.term).getOffset_skip_lists();
        return InvertedIndex.getSkip_lists().get(offset_sl);
    }

}
