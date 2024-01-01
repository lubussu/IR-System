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

    /**
     * Initialize the posting list iterator to the first element.
     */
    public void initList(){
        this.postingIterator = pl.iterator();
        if(!pl.isEmpty()) {
            this.actualPosting = postingIterator.next();
        }
    }

    public void addPosting(Posting p){
        this.pl.add(p);
    }

    /**
     * Goes to the next element in the posting list iterator.
     */
    public void next() {
        if (!postingIterator.hasNext()) {
            actualPosting = null;
        }else {
            actualPosting = postingIterator.next();
        }
    }

    /**
     * Goes to the next element in the posting list iterator greater or equal than docID. In case of skipping lists
     * the posting list block with max docID greater or equal than docID is first loaded and then then the iterator is set.
     */
    public void nextGEQ(int docID) {

        // Check if there are skipping lists AND if case the last loaded block has MaxDocID < docID. In that
        // case another block is needed.
        if(Flags.isSkipping() && (this.pl.isEmpty() || this.pl.get(this.pl.size()-1).getDocId() < docID)){
            SkipList sl = getSkipList();

            // Iterate all the elements of the corresponding skipping lists
            for(SkipElem se : sl.getSl()){

                // The first skipping element found with maxDocID >= docId
                if(se.getMaxDocId() >= docID){
                    int starting_point = this.pl.size();

                    // Read and load in memory the block with that maxDocID
                    readSkippingBlock(se);

                    // Set the new starting point of the iterator at the beginning of the new block
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

        // Search for the next DocID, be it in a block or in the full posting list
        while (postingIterator.hasNext() && actualPosting.getDocId() < docID) {
            actualPosting = postingIterator.next();
        }
        if(actualPosting != null && actualPosting.getDocId() < docID) {
            actualPosting = null;
        }
    }

    /**
     * Read the object from a file in binary code. In this case the term is read first and then the updateFromBinFile()
     * is called.
     *
     * @param channel Channel to the file to read
     * @param skipping Flag if skipping lists are present
     * @throws IOException Error while opening the file channel
     * @return true if the term is found, false otherwise
     */
    public boolean FromBinFile(FileChannel channel, boolean skipping) throws IOException {
        String current_term = IOUtils.readTerm(channel);

        // Check if the term is in the Index or if is correct
        if (current_term==null || !current_term.equals(this.term)) { //non ho letto il termine cercato (so che non c'è)
            return false;
        } else {
            updateFromBinFile(channel, skipping);
        }
        return true;
    }

    /**
     * Read the posting list length and read and calls the readCompressedPL() or readPL(),
     * depending on the isCompressed() flag.
     *
     * @param channel Channel to the file to read
     * @param skipping Flag if skipping lists are present
     * @throws IOException Error while opening the file channel
     */
    public void updateFromBinFile(FileChannel channel, boolean skipping) throws IOException {

        // Initialize the number of blocks to 1
        int num_blocks = 1;
        SkipList sl = null;

        // If there are skipping lists the number of blocks its initialized to the effective number of blocks of a posting list,
        // on the contrary the number of blocks remains 1 as there is only the full posting list to read.
        if(skipping){
            sl = getSkipList();
            num_blocks = sl.getSl().size();
        }
        ByteBuffer buffer = ByteBuffer.allocate(4);
        channel.read(buffer);
        buffer.flip();

        // Dimension of the full posting list
        int pl_size = buffer.getInt();

        // Iterate all the blocks (one time if is there are no skipping lists or if the block is only one)
        for(int i=0; i<num_blocks; i++){
            pl_size = skipping? sl.getSl().get(i).getBlock_size():pl_size;
            if (Flags.isCompression()) {
                readCompressedPL(channel, pl_size);
            } else{
                readPL(channel, pl_size);
            }
        }
    }

    /**
     * Read the posting list block corresponding to the passed skipping element from file.
     *
     * @param se Skipping element corresponding to the posting list block
     * @throws RuntimeException Error while opening the file channel
     */
    public void readSkippingBlock(SkipElem se){
        int block_number = InvertedIndex.getDictionary().get(term).getBlock_number();
        String filename = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged"+block_number;
        FileChannel channel = IOUtils.getFileChannel(filename, "read");

        try {

            // Set the channel offset to the one present in the skipping element,
            // that is the one at which the block starts
            channel.position(se.getBlockStartingOffset());

            // Checks if the Inverted Index is compressed and calls the respective function
            if (Flags.isCompression()) {
                readCompressedPL(channel, se.getBlock_size());
            } else{
                readPL(channel, se.getBlock_size());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the DocIDs and frequencies of a posting list (full or a block) written in a compressed format.
     * The compression algorithms used were variableBytes for the DocIDs and Unary for the frequencies.
     *
     * @param channel Channel to the file to read
     * @param pl_size Size of the posting list to read, be it a full posting list or a block
     * @throws IOException Error while opening the file channel
     */
    private void readCompressedPL(FileChannel channel, int pl_size) throws IOException {

        // First read the dimension in bytes of the posting list to read
        ByteBuffer buffer_pl = ByteBuffer.allocate(4);
        channel.read(buffer_pl);
        buffer_pl.flip();
        int byteRead = buffer_pl.getInt();
        if(byteRead < 0)
            return;

        // Then allocate a buffer to read all the bytes of the compressed posting list
        buffer_pl = ByteBuffer.allocate(byteRead);
        channel.read(buffer_pl);
        buffer_pl.flip();

        // Read the DocIDs of the posting list decompressing them with VariableBytes
        byte[] bytes = buffer_pl.array();
        ArrayList<Integer> docids = VariableByte.fromVariableBytesToIntegers(bytes,pl_size);

        /*****************************************************************************/
        int starting_unary = docids.remove(0);
        bytes = Arrays.copyOfRange(bytes, starting_unary, bytes.length);

        // Decompress the frequencies of the posting list with Unary decompression
        ArrayList<Integer> freqs = Unary.fromUnaryToInt(bytes);
        assert docids.size() == freqs.size();

        // Populate the posting list with the read data
        for (int j = 0; j < pl_size; j++) {
            int docid = docids.get(j);
            int freq = freqs.get(j);
            this.addPosting(new Posting(docid, freq));
        }

        initList();
        buffer_pl.clear();
    }

    /**
     * Read the DocIDs and frequencies of a posting list (full or a block) written in a uncompressed format.
     *
     * @param channel Channel to the file to read
     * @param pl_size Size of the posting list to read, be it a full posting list or a block
     * @throws IOException Error while opening the file channel
     */
    private void readPL(FileChannel channel, int pl_size) throws IOException {
        ByteBuffer buffer_pl = ByteBuffer.allocate(pl_size * 8);
        channel.read(buffer_pl);
        buffer_pl.flip();

        // Read simultaneously the DocIDs and the frequencies using half the block dimension as offset
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

    /**
     * Write the object to a file in binary code. In this case the term is read first and then the writeCompressedPlL() or writePL() is called,
     * depending on the isCompression() flag.
     *
     * @param channel Channel to the file to read
     * @param skipping Flag if skipping lists are present
     * @throws RuntimeException Error while opening the file channel
     */
    public void ToBinFile(FileChannel channel, boolean skipping) {
        try {

            // Write first the term on the posting list file. The writing_pl boolean tells whether to save the offset
            // of the posting list or skipping list in the dictionary
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

    /**
     * Write the DocIDs and frequencies of a posting list (full or a block) in a compressed format.
     * The compression algorithms used were variableBytes for the DocIDs and Unary for the frequencies.
     *
     * @param channel Channel to the file to read
     * @throws RuntimeException Error while opening the file channel
     */
    private void writeCompressedPL(FileChannel channel, boolean skipping) {

        // Open the file channel of the skipping lists file if the isSkipping() flag is true
        try (FileChannel skipChannel = skipping ?
                FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo.bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND) : null)
        {

            // Initialize the array list to populate with DocIDs and frequencies to write
            ArrayList<Integer> docids = new ArrayList<>();
            ArrayList<Integer> freqs = new ArrayList<>();
            SkipList sl = new SkipList(this.term);
            ByteBuffer buffer;
            int numBytes;

            // Checks if the block size of the skipping list regarding this posting list is bigger of the minimum default size
            int blockSize = Math.max( (int) Math.floor(Math.sqrt(this.pl.size())), Flags.getMinBlockSize() );

            // For each posting of the posting list
            for (Posting p : this.pl) {
                docids.add(p.getDocId());
                freqs.add(p.getTermFreq());

                // If the isSkipping() flag is true and the size of the current read postings % block size is 0,
                // meaning that i have block size postings
                if (skipping && docids.size() % blockSize == 0) {

                    // Compress the frequencies and DocIDs read till now and saves their length
                    byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
                    byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
                    numBytes = freqsCompressed.length + docsCompressed.length;

                    // Create the skipping element to write with le last DocID (max) of the ones read till now,
                    // the starting position of the block inside the postingList and the dimension of the block
                    // (different only in the last iteration)
                    SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position(), docids.size());
                    sl.addSkipElem(se);

                    // Put at the beginning of the block the number of bytes to read
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

            // If there are no more DocIDs to write, write the skipping list to file
            if (docids.isEmpty()) {
                if (skipChannel != null) {
                    sl.ToBinFile(skipChannel);
                }
                return;
            }

            // Else compress the postings of the last block
            byte[] freqsCompressed = Unary.fromIntToUnary(freqs);
            byte[] docsCompressed = VariableByte.fromIntegersToVariableBytes(docids);
            numBytes = freqsCompressed.length + docsCompressed.length;

            // Save the last skipping element and write the skipping list to file
            if (skipping && skipChannel != null) {
                SkipElem se = new SkipElem(docids.get(docids.size() - 1), channel.position(), docids.size());
                sl.addSkipElem(se);
                sl.ToBinFile(skipChannel);
            }

            // Write the last block to file
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

    /**
     * Write the DocIDs and frequencies of a posting list (full or a block) in a uncompressed format.
     *
     * @param channel Channel to the file to read
     * @throws RuntimeException Error while opening the file channel
     */
    private void writePL(FileChannel channel, boolean skipping) {

        // Open the file channel to the skipping file if there are skipping lists
        try(FileChannel skipChannel = skipping?
                FileChannel.open(Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo.bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND):null)
        {
            SkipList sl = new SkipList(this.term);
            // Check if the block size of the posting list (sqrt(pl.size()) is less then MinBlockSize() to avoid lots of small blocks
            // If there are skipping lists
            // Take the minimum between the posting list size and the MinBlockSize to write the block
            int size = skipping ? Math.max((int) Math.floor(Math.sqrt(this.pl.size())), Flags.getMinBlockSize()) : this.pl.size();
            ByteBuffer buffer = skipping ? ByteBuffer.allocate(4 + size * 8) : ByteBuffer.allocate(size * 8);

            int post_count = 0;
            // Frequencies starting position in the buffer
            int offset = (size-1) * 4; //freqs starting position in the buffer

            if (skipping) {
                buffer.putInt(size); //
            }

            // Iterate all postings
            for (Posting post : this.pl) {
                buffer.putInt(post.getDocId()); //docId
                int current_position = buffer.position();
                buffer.putInt(current_position + offset, post.getTermFreq()); //freq
                buffer.position(current_position);

                post_count++;
                if (skipping && post_count % size == 0) {
                    SkipElem se = new SkipElem(post.getDocId(), channel.position(), size);
                    sl.addSkipElem(se);

                    // Write the buffer to the file
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();

                    int toProcess = this.pl.size()-post_count;
                    if (toProcess == 0){
                        break;
                    }
                    int next_size = (toProcess >= size)? size : toProcess % size;
                    offset = (next_size-1) * 4;
                    buffer.putInt(next_size);
                }
            }
            if (skipping && post_count % size == 0) { //non ho un altro skipElem da aggiungere ma devo scrivere la skipList
                sl.ToBinFile(skipChannel);
                return;
            } else if (skipping && post_count % size != 0) {
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

    /**
     * Write the object to a file in text format.
     *
     * @param filename Path of the file to write
     */
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

    /**
     * Write the object to terminal.
     */
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

    /**
     * Return the skipping list related to this posting list (and its term).
     */
    public SkipList getSkipList(){
        int offset_sl = InvertedIndex.getDictionary().get(this.term).getOffset_skip_lists();
        return InvertedIndex.getSkip_lists().get(offset_sl);
    }

}
