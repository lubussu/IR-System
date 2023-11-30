package it.unipi.mircv;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.bean.*;
import it.unipi.mircv.utils.TextPreprocesser;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import static java.lang.Math.log;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class InvertedIndex {
    private static HashMap<String, DictionaryElem> dictionary = new HashMap<>();
    private static ArrayList<DocumentElem> docTable = new ArrayList<>();
    private static ArrayList<PostingList> posting_lists = new ArrayList<>();
    private static ArrayList<SkipList> skip_lists = new ArrayList<>();
    private static ArrayList<String> termList = new ArrayList<>();
    private static int block_number = 0;

    public static void clearIndexMem(){
        posting_lists.clear();
    }
    public static void clearDictionaryMem(){
        dictionary.clear();
    }

    public static void buildCachePostingList(){
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        PriorityQueue<DictionaryElem> queue_dict = new PriorityQueue<>((a, b) -> b.compareTo(a));

        System.out.println("(INFO) Starting creation posting list cache\n");
        clearIndexMem();
        System.gc();

        for (Map.Entry<String, DictionaryElem> entry : dictionary.entrySet()) {
            queue_dict.add(entry.getValue());
        }

        ArrayList<FileChannel> channels = new ArrayList<>();
        if(Flags.isSkipping()) {
            channels.add(IOUtils.getFileChannel(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo", "read"));
        } else{
            IOUtils.prepareChannels("", block_number);
        }

        while (!queue_dict.isEmpty()){
            DictionaryElem current_de = queue_dict.poll();
            if(Flags.isSkipping()) {
                try{
                    SkipList sl_to_cache = IOUtils.readSLFromFile(channels.get(0), current_de.getOffset_block_sl(),
                            current_de.getTerm());
                    skip_lists.add(sl_to_cache);
                    dictionary.get(sl_to_cache.getTerm()).setOffset_skip_lists(skip_lists.size() - 1);
                } catch (NullPointerException e){
                    System.out.println(current_de.getTerm());
                }
            }else{
                PostingList pl_to_cache = IOUtils.readPlFromFile(channels.get(current_de.getBlock_number()), current_de.getOffset_block_pl(),
                        current_de.getTerm());
                posting_lists.add(pl_to_cache);
                dictionary.get(pl_to_cache.getTerm()).setOffset_posting_lists(posting_lists.size() - 1);
            }

            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory){
                break;
            }
        }

        System.out.printf("(INFO) MAXIMUM CACHE MEMORY ACHIEVED. SAVING CACHE ON DISK\n" +
                "The number of term lists cached is %d\n", Flags.isSkipping()?skip_lists.size():posting_lists.size());
        String path;
        if(Flags.isSkipping()) {
            path = IOUtils.PATH_TO_FINAL_BLOCKS + "/SkippingListCache.bin";
        }else{
            path = IOUtils.PATH_TO_FINAL_BLOCKS + "/PostingListCache.bin";
        }
        if (!IOUtils.writeMergedDataToDisk(Flags.isSkipping()? skip_lists : posting_lists, path)) {
            System.out.println("(ERROR): Cache write to disk failed\n");
        } else {
            System.out.println("(INFO) Writing cache completed\n");
        }
    }

    public static void buildIndexFromFile(String filePath) {
        IOUtils.cleanDirectory(IOUtils.PATH_TO_TEMP_BLOCKS); //create or flush the directory
        ArrayList<String> tokens;
        HashSet<String> terms = new HashSet<>();
        int freq;
        int docid = -1;
        long total_lenght = 0;
        String docNo;
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        long start = System.currentTimeMillis();
        System.out.println("(INFO) Starting building index\n");

        //InputStreamReader permette di specificare la codifica da utilizzare
        //FileReader utilizza la codifica standard del SO usato
        try (TarArchiveInputStream collectionTar = new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(Paths.get(filePath))));
                BufferedReader br = new BufferedReader(new InputStreamReader(collectionTar, StandardCharsets.UTF_8))){
            String line;
            TarArchiveEntry entry = collectionTar.getNextTarEntry();
            System.out.println("Processing collection: " + entry.getName());
            while ((line = br.readLine()) != null) {
                tokens = TextPreprocesser.executeTextPreprocessing(line);

                docid = docid + 1;
                docNo = tokens.get(0);
                tokens.remove(0);

                docTable.add(new DocumentElem(docid, docNo, tokens.size()));
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
                    System.out.println("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.");
                    if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number)){
                        System.out.printf("(ERROR): %d block write to disk failed\n", block_number);
                        break;
                    }else {
                        System.out.printf("(INFO) Writing block '%d' completed\n\n", block_number);
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
            System.out.println("(INFO) Proceed with writing the final block to disk");
            if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number)) {
                System.out.println("(ERROR): final block write to disk failed");
            }else {
                System.out.printf("(INFO) Final block write %d completed\n\n", block_number);
                block_number++;
            }
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        CollectionInfo.setCollection_size(docid+1);
        CollectionInfo.setCollection_total_len(total_lenght);
        CollectionInfo.ToBinFile(IOUtils.getFileChannel("final/CollectionInfo", "write"));
        IOUtils.writeDocTable(docTable);

        termList = new ArrayList<>(terms);

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("(INFO) Indexing operation executed in: " + time + " minutes\n");
        writeTermList();
    }

    public static void mergeDictionary(ArrayList<String> termList) throws IOException {
        System.out.println("(INFO) Starting merging dictionary");
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
            }
        }
        System.out.println("(INFO) Merging dictionary completed\n");
    }

    public static void mergePostingList(ArrayList<String> termList) throws IOException {
        System.out.println("(INFO) Starting Merging PL\n");
        ArrayList<FileChannel> postingListChannels = IOUtils.prepareChannels("indexBlock", block_number);
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        int pl_block = 0;

        for (String term : termList) {
            PostingList posting_list = new PostingList(term);
            for (int i = 0; i < block_number; i++) {
                FileChannel channel = postingListChannels.get(i);
                long current_position = channel.position(); //conservo la posizione per risettarla se non leggo il termine cercato
                if (!posting_list.FromBinFile(channel))
                    channel.position(current_position);
            }
            posting_lists.add(posting_list);
            dictionary.get(term).setOffset_posting_lists(posting_lists.size()-1);
            dictionary.get(term).setBlock_number(pl_block);
            dictionary.get(term).computeMaxTf(posting_list);
            dictionary.get(term).computeMaxTFIDF();
            dictionary.get(term).computeMaxBM25(posting_list);

            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\nNumber of posting lists to write: %d\n",
                        posting_lists.size());
                /* Write block to disk */
                if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
                    System.out.printf("(ERROR): %d block write to disk failed\n", pl_block);
                    break;
                } else {
                    System.out.printf("(INFO) Writing block '%d' completed\n\n", pl_block);
                    pl_block++;
                }
                posting_lists.clear();
                System.gc();
            }
        }
        /* Write final block to disk */
        System.out.println("(INFO) Proceed with writing the final block to disk");
        if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
            System.out.println("(ERROR): Final block write to disk failed");
        } else {
            System.out.println("(INFO) Writing final block completed\n");
            pl_block++;
        }
        block_number = pl_block;
        System.out.println("(INFO) Merging PL completed\n");
    }

    public static void mergeIndexes(){
        IOUtils.cleanDirectory(IOUtils.PATH_TO_FINAL_BLOCKS); //clean directory of final files
        clearDictionaryMem();
        clearIndexMem();

        long start = System.currentTimeMillis();
        if(termList == null || termList.isEmpty())
            readTermList();
        try {
            mergeDictionary(termList);
            mergePostingList(termList);
            IOUtils.readDocTable();

            if (!IOUtils.writeMergedDictToDisk(new ArrayList<>(dictionary.values()))) {
                System.out.println("(ERROR): Merged dictionary write to disk failed");
            }else{
                System.out.println("(INFO) Merged dictionary write completed\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("(INFO)Merging operation executed in: " + time + " minutes\n");
    }

    public static void readDictionary(){
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS,"dictionaryMerged.bin");
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
        System.out.println("(INFO) Starting reading index");
        long start = System.currentTimeMillis();
        if(termList == null || termList.isEmpty())
           readTermList();

        readDictionary(); //read final Dictionary from file
        if(Flags.isSkipping()){
            readSLsFromFile("SkippingListCache.bin");
        }else {
            readPLsFromFile("PostingListCache.bin");
        }
        try {
            CollectionInfo.FromBinFile(IOUtils.getFileChannel("final/CollectionInfo", "read"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        IOUtils.readDocTable();

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nReading operation executed in: " + time + " minutes");
    }

    public static void readPLsFromFile(String filename){ //usata quando carico indice da file
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS, filename);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;
            while((current_term = IOUtils.readTerm(channel))!=null){
                PostingList pl = new PostingList(current_term);
                pl.updateFromBinFile(channel);
                posting_lists.add(pl);
                dictionary.get(current_term).setOffset_posting_lists(posting_lists.size()-1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readSLsFromFile(String filename){
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS, filename);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;
            while((current_term = IOUtils.readTerm(channel))!=null){
                SkipList sl = new SkipList(current_term);
                sl.updateFromBinFile(channel);
                skip_lists.add(sl);
                dictionary.get(current_term).setOffset_skip_lists(skip_lists.size()-1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public static void sortLastN(int n) {
        if (n > posting_lists.size()) {
            n = posting_lists.size();
        }

        ArrayList<PostingList> lastN = new ArrayList<>(posting_lists.subList(posting_lists.size() - n, posting_lists.size()));

        Comparator<PostingList> comparator = Comparator.comparingInt(pl -> pl.getPl().size());
        Collections.sort(lastN, comparator.reversed());

        for (int i = 0; i < n; i++) {
            posting_lists.set(posting_lists.size() - n + i, lastN.get(i));
        }
    }

    public static void updateCachePostingList(PostingList termPL, ArrayList<String> queryTerms){
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        if (Runtime.getRuntime().totalMemory() > MaxUsableMemory){
            for(int i=posting_lists.size()-1; i>=0; i--){
                if (!queryTerms.contains(posting_lists.get(i).getTerm())){
                    PostingList old_PL = posting_lists.get(i);
                    posting_lists.set(i, termPL);

                    dictionary.get(old_PL.getTerm()).setOffset_posting_lists(-1);
                    dictionary.get(termPL.getTerm()).setOffset_posting_lists(i);

                    sortLastN(posting_lists.size()-i);
                }
            }
        } else{
            posting_lists.add(posting_lists.size(), termPL);
            dictionary.get(termPL.getTerm()).setOffset_posting_lists(posting_lists.size());
        }
    }

    public static void writeTermList() {
        Collections.sort(termList);
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

    public static ArrayList<DocumentElem> getDocTable() {
        return docTable;
    }

    public static void setDocTable(ArrayList<DocumentElem> docTable) {
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

}
