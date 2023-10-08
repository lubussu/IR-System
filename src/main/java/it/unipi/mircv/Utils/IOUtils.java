package it.unipi.mircv.Utils;

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


public class IOUtils {

    public static boolean compression = true;

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
            else if (block==0)
                for (File file : folder.listFiles())
                    file.delete();

            block_pl.ToBinFile("temp/indexBlock" + block + ".bin", compression);
            block_de.ToBinFile("temp/dictionaryBlock" + block + ".bin");
        }
        return true;
    }

    public static void readBinBlockFromDisk(ArrayList<String> termList){
        try {
            mergeDictionary(termList);
            mergePostingList(termList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void mergeDictionary(ArrayList<String> termList) throws IOException {
        ArrayList<FileChannel> dictionaryChannels = new ArrayList<>();
        ArrayList<DictionaryElem> completeDictionary = new ArrayList<>();

        // Add FileChannel objects to the ArrayList
        for (int i = 0; i < 5; i++) {
            String path = "temp/dictionaryBlock" + i + ".bin";
            try {
                FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
                dictionaryChannels.add(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ByteBuffer buffer_size = ByteBuffer.allocate(4);

        for (String term : termList) {
            DictionaryElem dict = new DictionaryElem(term, 0, 0);
            for (int i = 0; i < 5; i++) {
                FileChannel channel = dictionaryChannels.get(i);
                long current_position = channel.position(); //conservo la posizione per risettarla se non leggo il termine cercato
                int byteRead = channel.read(buffer_size);
                if(byteRead == -1)
                    continue;
                buffer_size.flip();
                int termSize = buffer_size.getInt();
                ByteBuffer buffer_term = ByteBuffer.allocate(termSize+8);
                channel.read(buffer_term);
                buffer_term.flip();
                byte[] bytes = new byte[termSize];
                buffer_term.get(bytes);
                String current_term = new String(bytes, StandardCharsets.UTF_8);
                if (!current_term.equals(term)) { //non ho letto il termine cercato (so che non c'è)
                    channel.position(current_position); //setto la posizione
                } else {
                    int df = buffer_term.getInt();
                    int cf = buffer_term.getInt();
                    dict.setDf(dict.getDf() + df);
                    dict.setCf(dict.getCf() + cf);
                }
                buffer_size.clear();
                buffer_term.clear();
            }
            completeDictionary.add(dict);
        }
        for (DictionaryElem d : completeDictionary) {
            d.ToTextFile("temp/Dictionary.txt");
        }
    }

    public static void mergePostingList(ArrayList<String> termList) throws  IOException{
        ArrayList<FileChannel> postingListChannels = new ArrayList<>();
        ArrayList<PostingList> completePostingList = new ArrayList<>();
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        // Add FileChannel objects to the ArrayList
        for (int i = 0; i < 5; i++) {
            String path = "temp/indexBlock" + i + ".bin";
            try {
                FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
                postingListChannels.add(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (String term : termList) {
            PostingList posting_list = new PostingList(term);
            for (int i = 0; i < 5; i++) {
                FileChannel channel = postingListChannels.get(i);
                long current_position = channel.position(); //conservo la posizione per risettarla se non leggo il termine cercato
                if (!posting_list.FromBinFile(channel, compression))
                    channel.position(current_position);
            }

            completePostingList.add(posting_list);
            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.print("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n\n");
                System.out.println(completePostingList.size());
                break;
            }
        }
//        for(PostingList pl: completePostingList)
//            pl.printPostingList();
    }

}
