package it.unipi.mircv.Utils;

import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

public class IOUtils {

    public static boolean writeBinBlockToDisk(HashMap<String, DictionaryElem> blockDictionary,
                                              HashMap<String, PostingList> blockPostingList, int block) throws IOException{

        for (String term: blockDictionary.keySet()){

            PostingList block_pl = blockPostingList.get(term);
            DictionaryElem block_de = blockDictionary.get(term);

            block_pl.ToBinFile("temp/indexBlock" + block + ".bin");
            block_de.ToBinFile("temp/dictionaryBlock" + block + ".bin");

        }
        return true;
    }

    public static void readBinBlockFromDisk() throws IOException{

        try (FileChannel channel = FileChannel.open(Paths.get("temp/indexBlock0.bin"), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer);
            buffer.flip();
            int intValue = buffer.getInt();
            byte[] bytes = new byte[intValue];
            buffer.get(bytes);
            String term =  new String(bytes, StandardCharsets.UTF_8);
            int pl_size = buffer.getInt();
            int i = 0;
            System.out.print(term + ": ");
            while(i<pl_size){
                int docid = buffer.getInt();
                System.out.print("docid: " + docid + " ");
                i++;
            }
            while(i<pl_size*2){
                int tf = buffer.getInt();
                System.out.print("tf: " + tf + " ");
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
