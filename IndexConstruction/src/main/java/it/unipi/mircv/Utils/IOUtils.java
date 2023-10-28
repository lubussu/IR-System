package it.unipi.mircv.Utils;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.PostingList;

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

    public static void cleanDirectory(String directory){
        File folder = new File(directory);
        if (!folder.exists())
            folder.mkdirs();
        else
            for (File file : Objects.requireNonNull(folder.listFiles()))
                file.delete();
    }

    public static FileChannel getFileChannel(String filename, String mode) {
        return getFileChannel(filename, mode, "bin");
    }

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

    public static ArrayList<FileChannel> prepareChannels (String filename, int block_number){
        ArrayList<FileChannel> channels = new ArrayList<>();

        if(filename.isEmpty()){
            // Add FileChannel objects to the ArrayList
            for (int i = 0; i < block_number; i++) {
                Path path = Paths.get(PATH_TO_FINAL_BLOCKS, "/indexMerged" + i + ".bin");
                try {
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                    channels.add(channel);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }else {
            // Add FileChannel objects to the ArrayList
            for (int i = 0; i < block_number; i++) {
                Path path = Paths.get(PATH_TO_TEMP_BLOCKS, filename + i + ".bin");
                try {
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                    channels.add(channel);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return channels;
    }

    public static void readDocTable(){
        FileChannel channel = getFileChannel("final/DocumentTable", "read");
        DocumentElem doc = new DocumentElem();
        while(doc.FromBinFile(channel)){
            InvertedIndex.getDocTable().add(doc);
        }
    }

    public static PostingList readPlFromFile(FileChannel channel, long offset, String term){
        PostingList current_pl = new PostingList(term);
        try {
            channel.position(offset);
            if(current_pl.FromBinFile(channel, true))
                return current_pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String readTerm(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        int byteRead = channel.read(buffer);
        if(byteRead < 0)
            return null;

        buffer.flip();
        int termSize = buffer.getInt();
        buffer.clear();
        buffer = ByteBuffer.allocate(termSize);
        channel.read(buffer);
        buffer.flip();
        byte[] bytes = new byte[termSize];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

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

                block_pl.ToBinFile(index_channel, true);
                block_de.ToBinFile(dictionary_channel);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean writeMergedDataToDisk(ArrayList<?> mergedData, String filename) {
        File folder = new File(PATH_TO_FINAL_BLOCKS);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        try {
            FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            for (Object item : mergedData) {
                if (item instanceof PostingList) {
                    ((PostingList) item).ToBinFile(channel, true);
                } else if (item instanceof DictionaryElem) {
                    ((DictionaryElem) item).ToBinFile(channel);
                } // Add more cases for other data types if needed
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public static boolean writeMergedDictToDisk(ArrayList<DictionaryElem> mergedDictionary){
        String filename = PATH_TO_FINAL_BLOCKS + "/dictionaryMerged.bin";
        return writeMergedDataToDisk(mergedDictionary,filename);
    }

    public static boolean writeMergedPLToDisk(ArrayList<PostingList> mergedPostingList, int block){
        String filename = PATH_TO_FINAL_BLOCKS + "/indexMerged" + block + ".bin";
        return writeMergedDataToDisk(mergedPostingList, filename);
    }

    public static void writeDocTable(ArrayList<DocumentElem> docTable){
        FileChannel channel = getFileChannel("final/DocumentTable", "append");
        for(DocumentElem doc: docTable)
            doc.ToBinFile(channel);
    }
}