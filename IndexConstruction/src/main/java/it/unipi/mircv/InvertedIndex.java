package it.unipi.mircv;
import it.unipi.mircv.test.*;
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

    /**
     * Cleans the dictionary and posting lists arrays.
     */
    public static void clearMemory(){
        dictionary.clear();
        posting_lists.clear();
    }

    /**
     * Build the posting lists to cache at the end of the indexing. The cached posting lists are the ones with the highest
     * term frequencies.
     */
    public static void buildCachePostingList(){

        // Set the limit to the max usable memory before writing on disk
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        // Priority queue to store the posting lists with max term frequency
        PriorityQueue<DictionaryElem> queue_dict = new PriorityQueue<>((a, b) -> b.compareTo(a));

        System.out.println("(INFO) Starting Cache creation\n");
        posting_lists.clear();
        skip_lists.clear();
        System.gc();

        // If there are skipping lists read the posting list using them by first opening the file channel to the skipping list file
        FileChannel skipChannel = Flags.isSkipping()? IOUtils.getFileChannel(IOUtils.PATH_TO_FINAL_BLOCKS + "/SkipInfo", "read"): null;

        // Read each dictionary entry
        for (Map.Entry<String, DictionaryElem> entry : dictionary.entrySet()) {

            // Populate the priority queue
            queue_dict.add(entry.getValue());

            // Read the skipping list thanks the saved starting offset
            if(Flags.isSkipping()) {
                SkipList sl = IOUtils.readSLFromFile(skipChannel, entry.getValue().getOffset_block_sl(),
                        entry.getValue().getTerm());

                // Add the skipping list read and set the offset on the array in the dictionary
                skip_lists.add(sl);
                dictionary.get(sl.getTerm()).setOffset_skip_lists(skip_lists.size() - 1);
            }
        }

        // Open the file channels to the index files
        ArrayList<FileChannel> channels = IOUtils.prepareChannels(IOUtils.PATH_TO_FINAL_BLOCKS, "/indexMerged", block_number);

        // Till the queue is empty
        while (!queue_dict.isEmpty()){
            DictionaryElem current_de = queue_dict.poll();

            // Save the ordered posting lists in cache by reading them from the index
            PostingList pl_to_cache = IOUtils.readPlFromFile(channels.get(current_de.getBlock_number()), current_de.getOffset_block_pl(),
                    current_de.getTerm());
            posting_lists.add(pl_to_cache);

            // Set the offset of the posting list in cache in the dictionary
            dictionary.get(pl_to_cache.getTerm()).setOffset_posting_lists(posting_lists.size() - 1);

            // When the memory is full stop building the cache
            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory){
                System.out.println("(INFO) MAXIMUM CACHE MEMORY ACHIEVED. SAVING CACHE ON DISK");
                break;
            }
        }

        System.out.printf("(INFO) The number of term lists cached is %d\n\n", posting_lists.size());
        String path = IOUtils.PATH_TO_FINAL_BLOCKS + "/PostingListCache";

        // Save the cache on file
        if (!IOUtils.writeMergedDataToDisk(posting_lists, path)) {
            System.out.println("(ERROR): Cache write to disk failed\n");
        } else {
            System.out.println("(INFO) Writing cache completed\n");
        }
    }


    /**
     * Build the temporary index files by parsing the full document collection archive in filePath.
     * @param filePath Path to the archive containing the collection
     */
    public static void buildIndexFromFile(String filePath) {

        // Initialize the writing path, parameters and maximum usable memory
        // Create or flush the directory
        IOUtils.cleanDirectory(IOUtils.PATH_TO_TEMP_BLOCKS);
        ArrayList<String> tokens;
        HashSet<String> terms = new HashSet<>();
        int freq;
        int docid = -1;
        long total_lenght = 0;
        String docNo;
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        long start = System.currentTimeMillis();
        System.out.println("(INFO) Starting building index\n");

        // Read the entries of the tar archive and reads them by decompressing from a Gzip archive
        try (TarArchiveInputStream collectionTar = new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(Paths.get(filePath))));
                BufferedReader br = new BufferedReader(new InputStreamReader(collectionTar, StandardCharsets.UTF_8))){
            String line;

            // Take the first (and only) entry
            TarArchiveEntry entry = collectionTar.getNextTarEntry();
            System.out.println("Processing collection: " + entry.getName());

            // For each line of the document collection
            while ((line = br.readLine()) != null) {

                // Execute the text preprocessing (tokenization, stop-words removal, stemming etc.)
                tokens = TextPreprocesser.executeTextPreprocessing(line);

                docid = docid + 1;
                docNo = tokens.get(0);
                tokens.remove(0);

                // Create a new entry in the document table
                docTable.add(new DocumentElem(docid, docNo, tokens.size()));
                total_lenght += tokens.size();

                // For each token of the current line
                for (String term : tokens) {

                    // Count the frequency of the token in the line
                    freq = Collections.frequency(tokens, term);

                    // If it is a new token
                    if (!dictionary.containsKey(term)) {

                        // Create and add the term entry with the computed metrics to the termList, Dictionary e Posting list
                        terms.add(term);
                        dictionary.put(term, new DictionaryElem(term, 1,freq));
                        posting_lists.add(new PostingList(term, new Posting(docid, freq)));
                        dictionary.get(term).setOffset_posting_lists(posting_lists.size()-1);
                        dictionary.get(term).setBlock_number(block_number);
                        dictionary.get(term).setIdf(log(dictionary.size()));
                    }else {

                        // If there is already the term get the relative entries
                        DictionaryElem dict = dictionary.get(term);
                        ArrayList<Posting> pl = posting_lists.get(dict.getOffset_posting_lists()).getPl();

                        // Update the respective entries in the dictionary and add a new posting
                        if (pl.get(pl.size()-1).getDocId() != docid){
                            dict.setDf(dictionary.get(term).getDf()+1);
                            dict.setCf(dictionary.get(term).getCf()+freq);
                            dict.setIdf(log(((double) dictionary.size()/dict.getDf())));

                            pl.add(new Posting(docid,freq));
                        }
                    }
                }

                // Execute till the max usable memory is reached
                if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.println("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.");

                    // Save on disk the temporary posting list and dictionary blocks
                    if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number)){
                        System.out.printf("(ERROR): %d block write to disk failed\n", block_number);
                        break;
                    }else {
                        System.out.printf("(INFO) Writing block '%d' completed\n\n", block_number);

                        // Increase the temporary block index
                        block_number++;
                    }

                    // Clear memory used for the elaboration of previous block
                    clearMemory();
                    System.gc();

                    // Check if total memory is greater than usable memory
                    while (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                        System.out.println();
                        Thread.sleep(100);
                    }
                }
                tokens.clear();
            }
            // Write the final block to disk
            System.out.println("(INFO) Proceed with writing the final block to disk");
            if (!IOUtils.writeBinBlockToDisk(dictionary, posting_lists, block_number)) {
                System.out.println("(ERROR): final block write to disk failed");
            }else {
                System.out.println("(INFO) Writing final block completed");
                block_number++;
            }
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        // Write the collection information to file
        CollectionInfo.setCollection_size(docid+1);
        CollectionInfo.setCollection_total_len(total_lenght);
        CollectionInfo.ToBinFile(IOUtils.getFileChannel("final/CollectionInfo", "write"));
        IOUtils.writeDocTable(docTable);

        // Write the full list of terms to file
        termList = new ArrayList<>(terms);

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("(INFO) Indexing operation executed in: " + time + " minutes\n");
        writeTermList();
    }

    /**
     * Build the final dictionary file by parsing all the dictionary temporary files.
     * @param termList ArrayList of terms to read in the dictionary files
     */
    public static void mergeDictionary(ArrayList<String> termList) throws IOException {
        System.out.println("(INFO) Starting merging dictionary");

        // Open the file channels to the different dictionary blocks
        ArrayList<FileChannel> dictionaryChannels = IOUtils.prepareChannels(IOUtils.PATH_TO_TEMP_BLOCKS, "dictionaryBlock", block_number);

        // For each term in the term list
        for (String term : termList) {

            // Create an entry and parse all the block searching for the term
            DictionaryElem dict = new DictionaryElem(term, 0, 0);
            for (int i = 0; i < block_number; i++) {
                FileChannel channel = dictionaryChannels.get(i);

                // Store the position in case the term is not found
                long current_position = channel.position();

                // If the term is found continue, otherwise reset the position
                if (!dict.FromBinFile(channel))
                    channel.position(current_position);

                // Compute Idf and add the new entry
                dict.setIdf(log(((double) CollectionInfo.getCollection_size() /dict.getDf())));
                dictionary.put(term, dict);
            }
        }
        System.out.println("(INFO) Merging dictionary completed\n");
    }

    /**
     * Build the final posting list files by parsing all the posting list temporary files. The merging create the skipping lists
     * if the isSkipping flag is true.
     * @param termList ArrayList of terms to read in the dictionary files
     */
    public static void mergePostingList(ArrayList<String> termList) throws IOException {
        System.out.println("(INFO) Starting Merging PL\n");

        // Open the file channels to the temporary posting list files
        ArrayList<FileChannel> postingListChannels = IOUtils.prepareChannels(IOUtils.PATH_TO_TEMP_BLOCKS, "indexBlock", block_number);

        // Set the max usable memory
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;
        int pl_block = 0;

        // For each term in the term list parse the temporary files searching for the partial posting lists of that term
        for (String term : termList) {
            PostingList posting_list = new PostingList(term);
            for (int i = 0; i < block_number; i++) {
                FileChannel channel = postingListChannels.get(i);

                // Store the position in case the term is not found
                long current_position = channel.position();
                if (!posting_list.FromBinFile(channel, false))
                    channel.position(current_position);
            }

            // Create the entry for the final posting list and compute all the parameter for its dictionary entry
            posting_lists.add(posting_list);
            dictionary.get(term).setOffset_posting_lists(posting_lists.size()-1);
            dictionary.get(term).setBlock_number(pl_block);
            dictionary.get(term).computeMaxTf(posting_list);
            dictionary.get(term).computeMaxTFIDF();
            dictionary.get(term).computeMaxBM25(posting_list);


            // If the maximum memory is reached, write the full posting lists read till now to on disk
            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\nNumber of posting lists to write: %d\n",
                        posting_lists.size());
                /* Write block to disk */
                if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
                    System.out.printf("(ERROR): %d block write to disk failed\n", pl_block);
                    break;
                } else {
                    System.out.printf("(INFO) Writing block '%d' completed\n\n", pl_block);

                    // Increase the number of blocks written
                    pl_block++;
                }
                posting_lists.clear();
                System.gc();
            }
        }
        // Write the final block to disk
        System.out.println("(INFO) Proceed with writing the final block to disk");
        if (!IOUtils.writeMergedPLToDisk(posting_lists, pl_block)) {
            System.out.println("(ERROR): Final block write to disk failed");
        } else {
            System.out.println("(INFO) Writing final block completed");
            pl_block++;
        }
        block_number = pl_block;
        System.out.println("(INFO) Merging PL completed\n");
    }

    /**
     * Utility functions that calls the mergeDictionary() and the mergePostingList().
     * @throws RuntimeException Error while opening the file channel
     */
    public static void mergeIndexes(){

        // Clean directory of final files
        IOUtils.cleanDirectory(IOUtils.PATH_TO_FINAL_BLOCKS);
        clearMemory();

        long start = System.currentTimeMillis();

        // Read the termList of the index
        if(termList == null || termList.isEmpty())
            readTermList();
        try {

            // Call the function to build the merged and final index
            CollectionInfo.FromBinFile(IOUtils.getFileChannel("final/CollectionInfo", "read")); //??

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
        System.out.println("(INFO) Merging operation executed in: " + time + " minutes\n");
    }

    /**
     * Read the dictionary from file.
     * @throws RuntimeException Error while opening the file channel
     */
    public static void readDictionary(){
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS,"dictionaryMerged.bin");

        // Open the file channel to the dictionary file
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;

            // For each term read and stores in memory its dictionary parameters
            while((current_term = IOUtils.readTerm(channel))!=null){
                DictionaryElem dict = new DictionaryElem(current_term);
                dict.updateFromBinFile(channel);
                dict.setIdf(log(((double) CollectionInfo.getCollection_size()/dict.getDf()))); //??
                // dict.setIdf(log(((double) dictionary.size()/dict.getDf())));
                dictionary.put(current_term, dict);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the index from file. If there are skipping lists load them in memory, otherwise loads only the cache.
     * @throws RuntimeException Error while opening the file channel
     */
    public static void readIndexFromFile(){
        System.out.println("(INFO) Starting reading index");
        long start = System.currentTimeMillis();
        if(termList == null || termList.isEmpty())
           readTermList();

        // Read the collection data from their respective files
        CollectionInfo.FromBinFile(IOUtils.getFileChannel("final/CollectionInfo", "read"));
        IOUtils.readDocTable();

        // Read final Dictionary from file, the skipping lists if the isSKipping flag is true and finally the posting list cache
        readDictionary();
        if(Flags.isSkipping()) {
            readSLsFromFile("SkipInfo.bin");
        }
        readPLsFromFile("PostingListCache.bin");

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nReading operation executed in: " + time + " minutes\n");
    }

    /**
     * Read all the posting lists present in a file. Usually used for loading the cache.
     * @param filename File where to read the posting lists
     * @throws RuntimeException Error while opening the file channel
     */
    public static void readPLsFromFile(String filename){
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS, filename);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;

            // For each posting list in a file store it in memory and update the dictionary in-memory offset
            while((current_term = IOUtils.readTerm(channel))!=null){
                PostingList pl = new PostingList(current_term);
                pl.updateFromBinFile(channel, Flags.isSkipping());
                posting_lists.add(pl);
                dictionary.get(current_term).setOffset_posting_lists(posting_lists.size()-1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read all the skipping lists present in a file. Usually used for loading the skipping list cache.
     * @param filename File where to read the skipping lists
     * @throws RuntimeException Error while opening the file channel
     */
    public static void readSLsFromFile(String filename){
        Path path = Paths.get(IOUtils.PATH_TO_FINAL_BLOCKS, filename);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)){
            String current_term;

            // For each skipping list in a file store it in memory and update the dictionary in-memory offset
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

    /**
     * Read the term list of the index from file.
     */
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

    /**
     * Update the posting list cache with the new term of the last executed query
     */
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

    /**
     * Write the term list of the index to file.
     */
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

    /**
     * Test the different functionalities of the Inverted Index components.
     */
    public static void test(){
        try {
            CollectionInfoTest.doTest();
            DictionaryElemTest.doTest();
            DocumentElemTest.doTest();
            PostingListTest.doTest();
            SkipListTest.doTest();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Flags.setTesting(false);
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

    public static ArrayList<SkipList> getSkip_lists() {
        return skip_lists;
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
