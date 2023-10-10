package it.unipi.mircv;
import it.unipi.mircv.Utils.IOUtils;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import lombok.Getter;
import lombok.Setter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

@Getter
@Setter
public class InvertedIndex {
    private HashMap<String, PostingList> posting_lists;
    private HashMap<String, DictionaryElem> dictionary;
    private HashMap<Integer, DocumentElem> docTable;
    private ArrayList<String> termList;
    private int block_number;
    private boolean compression;

    public InvertedIndex(boolean compression) {
        this.compression = compression;
        this.posting_lists = new HashMap<>();
        this.dictionary = new HashMap<>();
        this.docTable = new HashMap<>();
        this.block_number = 0;
    }
    public InvertedIndex() {
        this(false);
    }

    public void clearIndexMem(){
        posting_lists.clear();
    }
    public void clearDictionaryMem(){
        dictionary.clear();
    }

    public ArrayList<Integer> search(String query) {
        query = query.toLowerCase();
        PostingList pl = posting_lists.get(query);
        ArrayList<Integer> docids = new ArrayList<Integer>();
        for(Posting p : pl.getPl())
            docids.add(p.getDocId());
        return docids;
    }

    public void buildIndexFromFile(String filePath) {
        ArrayList<String> tokens;
        HashSet<String> terms = new HashSet<>();
        int freq;
        int docid = -1;
        String docNo;
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        long start = System.currentTimeMillis();
        //InputStreamReader permette di specificare la codifica da utilizzare
        //FileReader utilizza la codifica standard del SO usato
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8))) {
            String line;
            TextPreprocesser.stopwords_global= Files.readAllLines(Paths.get("IndexConstruction/src/main/resources/stopwords.txt"));

            while ((line = br.readLine()) != null) {
                tokens = TextPreprocesser.executeTextPreprocessing(line);

                docid = docid + 1;
                docNo = tokens.get(0);
                tokens.remove(0);

                //docTable.put(docid, new DocumentElem(docid, docNo, tokens.size()));

                for (String term : tokens) {
                    freq = Collections.frequency(tokens, term);

                    if (!dictionary.containsKey(term)) {
                        terms.add(term);
                        dictionary.put(term, new DictionaryElem(term, 1,freq));
                        posting_lists.put(term, new PostingList(term, new Posting(docid, freq)));

                    }else {
                        ArrayList<Posting> pl = posting_lists.get(term).getPl();

                        if (pl.get(pl.size()-1).getDocId() != docid){
                            dictionary.get(term).setDf(dictionary.get(term).getDf()+1);
                            dictionary.get(term).setCf(dictionary.get(term).getCf()+freq);
                            //calcolare statistiche termine

                            pl.add(new Posting(docid,freq));
                        }
                    }
                }

                if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n\n");
                    /* Write block to disk */
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

        termList = new ArrayList<>(terms);

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nIndexing operation executed in: " + time + " minutes");
        Collections.sort(termList);
    }

    public void writeTermList() {
        termList.add(0, Integer.toString(block_number));
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("termList.txt"))) {
            String termListAsString = String.join(" ", termList);
            bufferedWriter.write(termListAsString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readIndexFromFile(){
        if(termList == null || termList.isEmpty())
            readTermList();

        IOUtils.readBinBlockFromDisk(termList);
    }

    public void readTermList() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("termList.txt"))) {
            String termListAsString = bufferedReader.readLine();
            String[] termArray = termListAsString.split(" ");
            termList = new ArrayList<>(Arrays.asList(termArray));
            block_number = Integer.parseInt(termList.remove(0));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}