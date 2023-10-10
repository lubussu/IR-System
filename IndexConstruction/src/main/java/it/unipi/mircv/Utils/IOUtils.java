package it.unipi.mircv.Utils;

import it.unipi.mircv.bean.DictionaryElem;
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


public class IOUtils {
    public static String PATH_TO_TEMP_BLOCKS = "temp";
    public static String PATH_TO_FINAL_BLOCKS = "final";
    
    public static ArrayList<FileChannel> prepareChannels (String filename, int block_number){
        ArrayList<FileChannel> channels = new ArrayList<>();

        // Add FileChannel objects to the ArrayList
        for (int i = 0; i < block_number; i++) {
            Path path = Paths.get(PATH_TO_TEMP_BLOCKS, filename+ i + ".bin");
            try {
                FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                channels.add(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return channels;
    }

    public static boolean writeBinBlockToDisk(HashMap<String, DictionaryElem> blockDictionary,
                                              HashMap<String, PostingList> blockPostingList, int block) throws IOException{

        ArrayList<String> termList = new ArrayList<>(blockDictionary.keySet());
        Collections.sort(termList);

        File folder = new File(PATH_TO_TEMP_BLOCKS);
        if (!folder.exists())
            folder.mkdirs();
        else if (block==0)
            for (File file : folder.listFiles())
                file.delete();

        String dictionary_filename = PATH_TO_TEMP_BLOCKS + "/dictionaryBlock" + block + ".bin";
        String index_filename = PATH_TO_TEMP_BLOCKS + "/indexBlock" + block + ".bin";

        try (FileChannel dictionary_channel = FileChannel.open(Paths.get(dictionary_filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
             FileChannel index_channel = FileChannel.open(Paths.get(index_filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)){

            for (String term : termList) {
                PostingList block_pl = blockPostingList.get(term);
                DictionaryElem block_de = blockDictionary.get(term);

                block_pl.ToBinFile(index_channel, true);
                block_de.ToBinFile(dictionary_channel);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean writeMergedPLToDisk(ArrayList<PostingList> mergedPostingList, int block){

        File folder = new File(PATH_TO_FINAL_BLOCKS);
        if (!folder.exists())
            folder.mkdirs();
        else if (block==0)
            for (File file : folder.listFiles())
                file.delete();

        String filename = PATH_TO_FINAL_BLOCKS + "/indexMerged" + block + ".bin";

        try{
            FileChannel channel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            for(PostingList pl : mergedPostingList){
                pl.ToBinFile(channel, true);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    public static String readTerm(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        int byteRead = channel.read(buffer);
        if(byteRead == -1)
            return null;

        buffer.flip();
        int termSize = buffer.getInt();
        buffer = ByteBuffer.allocate(termSize);
        channel.read(buffer);
        buffer.flip();
        byte[] bytes = new byte[termSize];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    }