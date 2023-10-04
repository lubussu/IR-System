package it.unipi.mircv.Utils;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;

public class IOUtils {

    public static boolean writeBinBlockToDisk(HashMap<String, DictionaryElem> blockDictionary,
                                              HashMap<String, PostingList> blockPostingList, int block) throws IOException{
        ArrayList<String> termList = new ArrayList<>(blockDictionary.keySet());
        Collections.sort(termList);
        for (String term: termList){
            PostingList block_pl = blockPostingList.get(term);
            DictionaryElem block_de = blockDictionary.get(term);

            File folder = new File("temp");
            if (!folder.exists())
                folder.mkdirs();

            block_pl.ToBinFile("temp/indexBlock" + block + ".bin");
            block_de.ToBinFile("temp/dictionaryBlock" + block + ".bin");

        }
        return true;
    }

//    public static void readBinBlockFromDisk() throws IOException{
//
//        int i = 0;
//        PriorityQueue<OrderedList> pQueue = new PriorityQueue<>(SPIMI.block_number == 0 ? 1 : SPIMI.block_number, new ComparatorTerm());
//
//        while(i <= InvertedIndex.block_number)
//            try (FileChannel channel = FileChannel.open(Paths.get("temp/dictionaryBlock" + i + ".bin"), StandardOpenOption.READ)) {
//                int bufferSize = 1024; // You can adjust the buffer size as needed
//                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
//
//                // Read data from the file until the end
//                while ((channel.read(buffer)) != -1) {
//                    buffer.flip(); // Switch to read mode
//                    buffer.clear(); // Clear the buffer for the next read
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        try (FileChannel channel = FileChannel.open(Paths.get("temp/indexBlock0.bin"), StandardOpenOption.READ)) {
//            ByteBuffer buffer = ByteBuffer.allocate(1024);
//            int bytesRead = channel.read(buffer);
//            buffer.flip();
//            int intValue = buffer.getInt();
//            byte[] bytes = new byte[intValue];
//            buffer.get(bytes);
//            String term =  new String(bytes, StandardCharsets.UTF_8);
//            int pl_size = buffer.getInt();
//            int i = 0;
//            System.out.print(term + ": ");
//            while(i<pl_size){
//                int docid = buffer.getInt();
//                System.out.print("docid: " + docid + " ");
//                i++;
//            }
//            while(i<pl_size*2){
//                int tf = buffer.getInt();
//                System.out.print("tf: " + tf + " ");
//                i++;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }



}
