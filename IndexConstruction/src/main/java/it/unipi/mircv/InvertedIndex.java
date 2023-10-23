package it.unipi.mircv;
import it.unipi.mircv.Utils.IOUtils;
import it.unipi.mircv.bean.*;
import it.unipi.mircv.utils.Flags;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import static java.lang.Math.log;

public class InvertedIndex {
    private static HashMap<String, DictionaryElem> dictionary = new HashMap<>();
    private static HashMap<Integer, DocumentElem> docTable = new HashMap<>();
    private static ArrayList<PostingList> posting_lists = new ArrayList<>();
    private static ArrayList<String> termList = new ArrayList<>();
    private static int block_number = 0;
    private static boolean compression = true;

    public static void clearIndexMem(){
        posting_lists.clear();
    }
    public static void clearDictionaryMem(){
        dictionary.clear();
    }

    public static void buildIndexFromFile(String filePath) {
        IOUtils.cleanDirectory("temp"); //clean directory of temporary files
        ArrayList<String> tokens;
        HashSet<String> terms = new HashSet<>();
        int freq;
        int docid = -1;
        long total_lenght = 0;
        String docNo;
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        long start = System.currentTimeMillis();
        //InputStreamReader permette di specificare la codifica da utilizzare
        //FileReader utilizza la codifica standard del SO usato
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                tokens = TextPreprocesser.executeTextPreprocessing(line);

                docid = docid + 1;
                docNo = tokens.get(0);
                tokens.remove(0);

                //docTable.put(docid, new DocumentElem(docid, docNo, tokens.size()));
                total_lenght += tokens.size();

                for (String term : tokens) {
                    freq = Collections.frequency(tokens, term);

                    if (!dictionary.containsKey(term)) {
                        terms.add(term);
                        dictionary.put(term, new DictionaryElem(term, 1,freq));
                        posting_lists.add(new PostingList(term, new Posting(docid, freq)));
                        dictionary.get(term).setOffset_posting_lists(posting_lists.size()-1);
                        dictionary.get(term).setBlock_number(block_number);
                        dictionary.get(term).setIdf(log(dictionary.size()));
                    }else {
                        DictionaryElem dict = dictionary.get(term);
                        ArrayList<Posting> pl = posting_lists.get(dict.getOffset_posting_lists()).getPl();

                        if (pl.get(pl.size()-1).getDocId() != docid){
                            dict.setDf(dictionary.get(term).getDf()+1);
                            dict.setCf(dictionary.get(term).getCf()+freq);
                            dict.setIdf(log(((double) dictionary.size()/dict.getDf())));

                            pl.add(new Posting(docid,freq));
                        }
                    }
                }

                if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n\n");
                    if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number)){
                        System.out.printf("(ERROR): %d block write to disk failed\n", block_number);
                        break;
                    }else {
                        System.out.printf("(INFO) Writing block '%d' completed\n", block_number);
                        block_number++;
                    }

                    /* Clear memory used for the elaboration of previous block */
                    clearIndexMem();
                    clearDictionaryMem();
                    System.gc();

                    /* Check if total memory is greater than usable memory */
                    while (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                        System.out.println();
                        Thread.sleep(100);
                    }
                }

                tokens.clear();

            }
            /* Write the final block in memory */
            System.out.println("(INFO) Proceed with writing the final block to disk in memory");
            if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number))
                System.out.print("(ERROR): final block write to disk failed\n");
            else
                System.out.printf("(INFO) Final block write %d completed\n", block_number);
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        CollectionInfo.setCollection_size(docid+1);
        CollectionInfo.setCollection_total_len(total_lenght);
        CollectionInfo.ToBinFile(IOUtils.getFileChannel("final/CollectionInfo", "write"));

        termList = new ArrayList<>(terms);

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nIndexing operation executed in: " + time + " minutes");
        Collections.sort(termList);
    }

    public static void mergeDictionary(ArrayList<String> termList) throws IOException {
        System.out.printf("(INFO) Starting merging dictionary\n");
        ArrayList<FileChannel> dictionaryChannels = IOUtils.prepareChannels("dictionaryBlock", block_number);

        for (String term : termList) {
            DictionaryElem dict = new DictionaryElem(term, 0, 0);
            for (int i = 0; i < block_number; i++) {
                FileChannel channel = dictionaryChannels.get(i);
                long current_position = channel.position(); //conservo la posizione per risettarla se non leggo il termine cercato
                if (!dict.FromBinFile(channel))
                    channel.position(current_position);
                dict.setIdf(log(((double) dictionary.size()/dict.getDf())));
                dictionary.put(term, dict);
                //dict.ToTextFile("Dictionary.txt");
            }
        }

        System.out.printf("(INFO) Merging dictionary completed\n");
    }

    public static void mergePostingList(ArrayList<String> termList) throws IOException {
        System.out.printf("(INFO) Starting Merging PL\n");
        ArrayList<FileChannel> postingListChannels = IOUtils.prepareChannels("indexBlock", block_number);
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        int pl_block = 0;

        for (String term : termList) {
            PostingList posting_list = new PostingList(term);
            for (int i = 0; i < block_number; i++) {
                FileChannel channel = postingListChannels.get(i);
                long current_position = channel.position(); //conservo la posizione per risettarla se non leggo il termine cercato
                if (!posting_list.FromBinFile(channel, true))
                    channel.position(current_position);
            }
            posting_lists.add(posting_list);
            dictionary.get(term).setOffset_posting_lists(posting_lists.size()-1);
            dictionary.get(term).setBlock_number(pl_block);

            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n\n Number of posting lists to write: %d\n",
                        posting_lists.size());
                /* Write block to disk */
                if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
                    System.out.printf("(ERROR): %d block write to disk failed\n", pl_block);
                    break;
                } else {
                    System.out.printf("(INFO) Writing block '%d' completed\n", pl_block);
                    pl_block++;
                }
                posting_lists.clear();
                System.gc();
            }
        }
        /* Write final block to disk */
        if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
            System.out.printf("(ERROR): final block write to disk failed\n", pl_block);
        } else {
            System.out.printf("(INFO) Writing final block completed\n", pl_block);
        }
        block_number = pl_block;
        System.out.printf("(INFO) Merging PL completed\n");
    }

    public static void mergeIndexes(){
        IOUtils.cleanDirectory("final"); //clean directory of final files
        clearDictionaryMem();
        clearIndexMem();

        long start = System.currentTimeMillis();
        if(termList == null || termList.isEmpty())
            readTermList();
        try {
            mergeDictionary(termList);
            mergePostingList(termList);

            if (!IOUtils.writeMergedDictToDisk(new ArrayList<>(dictionary.values()), 0)) {
                System.out.printf("(ERROR): Merged dictionary write to disk failed\n");
            }else{
                System.out.printf("(INFO) Merged dictionary write completed\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nMerging operation executed in: " + time + " minutes");
    }

    public static void readPL(int block){
        Path path = Paths.get("final/indexMerged"+block+".bin");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;
            while((current_term = IOUtils.readTerm(channel))!=null){
                PostingList pl = new PostingList(current_term);
                pl.updateFromBinFile(channel, Flags.isCompression());
                posting_lists.add(pl);
                dictionary.get(current_term).setBlock_number(block);
                dictionary.get(current_term).setOffset_posting_lists(posting_lists.size()-1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readDictionary(){
        Path path = Paths.get("final","dictionaryMerged.bin");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;
            while((current_term = IOUtils.readTerm(channel))!=null){
                DictionaryElem dict = new DictionaryElem(current_term);
                dict.updateFromBinFile(channel);
                dict.setIdf(log(((double) dictionary.size()/dict.getDf())));
                dictionary.put(current_term, dict);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readIndexFromFile(){
        long start = System.currentTimeMillis();
        if(termList == null || termList.isEmpty())
            readTermList();

        readDictionary(); //read final Dictionary from file
        readPL(1); //read final PL block form file

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nReading operation executed in: " + time + " minutes");
    }

    public static void readTermList() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("termList.txt"))) {
            String termListAsString = bufferedReader.readLine();
            String[] termArray = termListAsString.split(" ");
            termList = new ArrayList<>(Arrays.asList(termArray));
            block_number = Integer.parseInt(termList.remove(0));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeTermList() {
        termList.add(0, Integer.toString(block_number));
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("termList.txt"))) {
            String termListAsString = String.join(" ", termList);
            bufferedWriter.write(termListAsString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HashMap<String, DictionaryElem> getDictionary() {
        return dictionary;
    }

    public static void setDictionary(HashMap<String, DictionaryElem> dictionary) {
        InvertedIndex.dictionary = dictionary;
    }

    public static HashMap<Integer, DocumentElem> getDocTable() {
        return docTable;
    }

    public static void setDocTable(HashMap<Integer, DocumentElem> docTable) {
        InvertedIndex.docTable = docTable;
    }

    public static ArrayList<PostingList> getPosting_lists() {
        return posting_lists;
    }

    public static void setPosting_lists(ArrayList<PostingList> posting_lists) {
        InvertedIndex.posting_lists = posting_lists;
    }

    public static ArrayList<String> getTermList() {
        return termList;
    }

    public static void setTermList(ArrayList<String> termList) {
        InvertedIndex.termList = termList;
    }

    public static int getBlock_number() {
        return block_number;
    }

    public static void setBlock_number(int block_number) {
        InvertedIndex.block_number = block_number;
    }

    public static boolean isCompression() {
        return compression;
    }

    public static void setCompression(boolean compression) {
        InvertedIndex.compression = compression;
    }

}
