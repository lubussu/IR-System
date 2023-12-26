package it.unipi.mircv.utils;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.SkipList;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;

public class IOUtils {
    public static final String PATH_TO_TEMP_BLOCKS = "temp/index";
    public static final String PATH_TO_FINAL_BLOCKS = "final/index";
    public static final String PATH_TO_TEST = "test";
    /**
     * Called at every index reconstruction to clean old index files in the directories.
     * @param directory Directory to clean
     */
    public static void cleanDirectory(String directory){
        File folder = new File(directory);
        if (!folder.exists())
            folder.mkdirs();
        else
            for (File file : Objects.requireNonNull(folder.listFiles()))
                file.delete();
    }

    /**
     * Close the passed file channel.
     * @param channel File channel to close
     */
    public static void closeChannel(FileChannel channel){
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the file channel of a file and opens it with the specified mode.
     * @param filename File to open
     * @param mode Mode with which the file channel is opened
     * @return The opened file channel
     */
    public static FileChannel getFileChannel(String filename, String mode) {
        return getFileChannel(filename, mode, "bin");
    }

    /**
     * Get the file channel of a file and opens it with the specified mode of the specified file type.
     * @param filename File to open
     * @param mode Mode with which the file channel is opened
     * @param fileType Extension of the file to open
     * @return The opened file channel
     */
    public static FileChannel getFileChannel(String filename, String mode, String fileType){
        Path path = Paths.get( filename+ "." + fileType);
        File file = path.toFile();
        File directory = file.getParentFile();

        if (!directory.exists()) {
            directory.mkdirs();
        }
        FileChannel channel = null;
        try {
            switch (mode) {
                case "read":
                    channel = FileChannel.open(path, StandardOpenOption.READ);
                    break;
                case "append":
                    channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                    break;
                case "write":
                    channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return channel;
    }

    /**
     * Open file channels for all the files in a directory, usually called to open all index blocks.
     * @param filePath Folder where the files are stored
     * @param filename File to open
     * @param block_number Block number of the file to open
     * @return An array list containing the file channels of the different blocks
     */
    public static ArrayList<FileChannel> prepareChannels (String filePath, String filename, int block_number){
        ArrayList<FileChannel> channels = new ArrayList<>();

        // Add FileChannel objects to the ArrayList
        for (int i = 0; i < block_number; i++) {
            Path path = Paths.get(filePath, filename + i + ".bin");

            try {
                FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                channels.add(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return channels;
    }

    /**
     * Read the file containing the Document Table.
     */
    public static void readDocTable(){
        FileChannel channel = getFileChannel("final/DocumentTable", "read");
        DocumentElem doc = new DocumentElem();
        while(doc.FromBinFile(channel)){
            InvertedIndex.getDocTable().add(doc);
        }
    }

    /**
     * Read the posting list from file saved at the specified offset.
     * @param channel Folder where the block file is stored
     * @param offset Offset on file of the posting list
     * @param term Term of the posting list to read
     * @return The read posting list
     */
    public static PostingList readPlFromFile(FileChannel channel, long offset, String term){
        PostingList current_pl = new PostingList(term);
        try {
            channel.position(offset);
            if(current_pl.FromBinFile(channel, Flags.isSkipping()))
                return current_pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Read the skipping list from file saved at the specified offset.
     * @param channel Folder where the block file is stored
     * @param offset Offset on file of the skipping list
     * @param term Term of the skipping list to read
     * @return The current skipping list
     */
    public static SkipList readSLFromFile(FileChannel channel, long offset, String term){
        SkipList current_sl = new SkipList(term);
        try {
            channel.position(offset);
            if(current_sl.FromBinFile(channel))
                return current_sl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Read the term at the start of the posting list.
     * @param channel Folder where the block file is stored
     * @return The term read of the posting list
     */
    public static String readTerm(FileChannel channel) throws IOException {

        // Read the dimension in bytes of the term
        ByteBuffer buffer = ByteBuffer.allocate(4);
        int byteRead = channel.read(buffer);
        if(byteRead < 0)
            return null;

        buffer.flip();

        // Read the term bytes
        int termSize = buffer.getInt();
        buffer.clear();
        buffer = ByteBuffer.allocate(termSize);
        channel.read(buffer);
        buffer.flip();
        byte[] bytes = new byte[termSize];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Write the term at the start of the posting list.
     * @param channel Folder where the block file is stored
     */
    public static void writeTerm(FileChannel channel, String term, int size, boolean writing_pl) throws IOException {

        // Allocate a buffer for the term dimension in byte, the term itself and the length of the posting list
        byte[] descBytes = String.valueOf(term).getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + descBytes.length + 4);
        long start_position = channel.position();
        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
        if(writing_pl) {

            // writing a posting_list
            dict.setOffset_block_pl(start_position);
        }else{

            // writing a skipping_list
            dict.setOffset_block_sl(start_position);
        }

        // Populate the buffer for termLenght + term
        buffer.putInt(descBytes.length);
        buffer.put(descBytes);

        // pl_size if writing PL, sl_size if writing SL
        buffer.putInt(size);
        buffer.flip();
        // Write the buffer to the file
        channel.write(buffer);
    }

    /**
     * Write an index block to disk with the current partial posting lists and dictionary in memory, usually used when building
     * the temporary Inverted Index.
     * @param blockDictionary Partial dictionary stored in memory
     * @param blockPostingList Partial posting lists stored in memory
     * @param block Number of the block to be stored on the disk
     * @throws IOException Error while opening the file channel
     */
    public static boolean writeBinBlockToDisk(HashMap<String, DictionaryElem> blockDictionary,
                                              ArrayList<PostingList> blockPostingList, int block) throws IOException{

        ArrayList<String> termList = new ArrayList<>(blockDictionary.keySet());
        Collections.sort(termList);

        String dictionary_filename = PATH_TO_TEMP_BLOCKS + "/dictionaryBlock" + block + ".bin";
        String index_filename = PATH_TO_TEMP_BLOCKS + "/indexBlock" + block + ".bin";

        try (FileChannel dictionary_channel = FileChannel.open(Paths.get(dictionary_filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
             FileChannel index_channel = FileChannel.open(Paths.get(index_filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){

            for (String term : termList) {
                DictionaryElem block_de = blockDictionary.get(term);
                PostingList block_pl = blockPostingList.get(block_de.getOffset_posting_lists());

                // During building phase write PL without skipping mode even if Flags.isSkipping() is true
                block_pl.ToBinFile(index_channel,false);
                block_de.ToBinFile(dictionary_channel);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * Write the final object to disk after merging the temporary files.
     * @param mergedData Array list of objects to write
     * @param filename File where to store the Array list
     */
    public static boolean writeMergedDataToDisk(ArrayList<?> mergedData, String filename) {
        File folder = new File(PATH_TO_FINAL_BLOCKS);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        try (FileChannel channel = FileChannel.open(Paths.get(filename + ".bin"), StandardOpenOption.CREATE, StandardOpenOption.APPEND))
            {
            for (Object item : mergedData) {
                if (item instanceof PostingList) {
                    ((PostingList) item).ToBinFile(channel, Flags.isSkipping());
                } else if (item instanceof DictionaryElem) {
                    ((DictionaryElem) item).ToBinFile(channel);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * Utility function that calls the writeMergedDataToDisk() for a Dictionary object.
     * @param mergedDictionary Array list of Dictionary elements to write
     */
    public static boolean writeMergedDictToDisk(ArrayList<DictionaryElem> mergedDictionary){
        String filename = PATH_TO_FINAL_BLOCKS + "/dictionaryMerged";
        return writeMergedDataToDisk(mergedDictionary, filename);
    }

    /**
     * Utility function that calls the writeMergedDataToDisk() for a Posting list array object.
     * @param mergedPostingList Array list of complete posting lists to write
     */
    public static boolean writeMergedPLToDisk(ArrayList<PostingList> mergedPostingList, int block){
        String filename = PATH_TO_FINAL_BLOCKS + "/indexMerged" + block;
        return writeMergedDataToDisk(mergedPostingList, filename);
    }

    /**
     * Write the document table elements to file.
     * @param docTable Document Table to store
     */
    public static void writeDocTable(ArrayList<DocumentElem> docTable){
        FileChannel channel = getFileChannel("final/DocumentTable", "append");
        for(DocumentElem doc: docTable)
            doc.ToBinFile(channel);
    }
}