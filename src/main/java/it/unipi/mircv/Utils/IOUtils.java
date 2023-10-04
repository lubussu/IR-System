package it.unipi.mircv.Utils;

import it.unipi.mircv.InvertedIndex;
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

    public static void readBinBlockFromDisk(ArrayList<String> termList) throws IOException {
        ArrayList<FileChannel> dictionaryChannels = new ArrayList<>();
        ArrayList<FileChannel> postingListChannels = new ArrayList<>();
        ArrayList<DictionaryElem> completeDictionary = new ArrayList<>();

        // Add FileChannel objects to the ArrayList
        for (int i = 0; i <= InvertedIndex.block_number; i++) {
            String path = "temp/dictionaryBlock" + i + ".bin";
            try (FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ)){
                dictionaryChannels.add(channel);
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        for(String term : termList){
            DictionaryElem dict = new DictionaryElem(term, 0, 0);
            for(int i = 0; i <= InvertedIndex.block_number; i++){
                FileChannel channel = dictionaryChannels.get(i);
                long current_position = channel.position();
                int bytesRead = channel.read(buffer);
                buffer.flip();
                int intValue = buffer.getInt();
                byte[] bytes = new byte[intValue];
                buffer.get(bytes);
                String current_term =  new String(bytes, StandardCharsets.UTF_8);
                if(!current_term.equals(term)){
                    channel.position(current_position);
                }else{
                    int df = buffer.getInt();
                    int cf = buffer.getInt();
                    dict.setDf(dict.getDf() + df);
                    dict.setCf(dict.getDf() + cf);
                }
                buffer.clear();
            }
            completeDictionary.add(dict);
        }

        for(DictionaryElem d : completeDictionary){
            d.ToTextFile("temp/Dictionary.txt");
        }

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
    }
}
