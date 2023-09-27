package it.unipi.mircv;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.DocumentElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class InvertedIndex {
    public static HashMap<String, PostingList> posting_lists = new HashMap<>();
    public static HashMap<String, DictionaryElem> dictionary = new HashMap<>();
    public static int block_number = 0;

    public InvertedIndex() {

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
        int freq;
        int docid = -1;
        String docNo;
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        //InputStreamReader permette di specificare la codifica da utilizzare
        //FileReader utilizza la codifica standard del SO usato
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            TextPreprocesser.stopwords_global= Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));

            while ((line = br.readLine()) != null) {
                tokens = TextPreprocesser.executeTextPreprocessing(line);

                docid = docid + 1;
                docNo = tokens.get(0);
                tokens.remove(0);

                DocumentElem doc = new DocumentElem(docid, docNo, tokens.size());

                for (String term : tokens) {
                    freq = Collections.frequency(tokens, term);

                    if (!dictionary.containsKey(term)) {

                        dictionary.put(term, new DictionaryElem(term, 1,freq));
                        posting_lists.put(term, new PostingList(term, new Posting(docid, freq)));

                    }else {
                        ArrayList<Posting> pl = posting_lists.get(term).getPl();

                        if (pl.isEmpty() || pl.get(pl.size()-1).getDocId() != docid){
                            dictionary.get(term).setDf(dictionary.get(term).getDf()+1);
                            dictionary.get(term).setCf(dictionary.get(term).getCf()+freq);
                            //calcolare statistiche termine

                            pl.add(new Posting(docid,freq));
                        }
                    }
                }

                if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n\n");
                    //writeAndClean();
                    break;
                }

                tokens.clear();

            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void writeAndClean(){
        for (Map.Entry<String, PostingList> entry : posting_lists.entrySet()) {
            PostingList pl = entry.getValue();
            pl.ToTextFile("posting_lists.txt");
        }

        for (Map.Entry<String, DictionaryElem> entry : dictionary.entrySet()) {
            DictionaryElem dic = entry.getValue();
            dic.ToTextFile("dictionary.txt");
        }

        dictionary.clear();
        posting_lists.clear();
        System.gc();
    }

    public static void main(String[] args) {
        String filePath = "src/main/resources/collection.tsv";

        InvertedIndex invertedIndex = new InvertedIndex();
        invertedIndex.buildIndexFromFile(filePath);


        String query = "engineers";
        ArrayList<Integer> results = invertedIndex.search(query);

        if (!results.isEmpty()) {
            System.out.println("Documents containing '" + query + "':");
            for (int doc : results) {
                System.out.println("Document " + doc + ": ");
            }
        } else {
            System.out.println("No documents found for '" + query + "'.");
        }

        /*
        for (Map.Entry<String, PostingList> entry : posting_lists.entrySet()) {
            String term = entry.getKey();
            PostingList pl = entry.getValue();
            System.out.printf("Term: %s\n", term);
            System.out.printf("PL (DocId, Freq):\n");
            for(Posting p: pl.getPl()){
                System.out.printf("(%d, %d) \t", p.getDocId(), p.getTermFreq());
            }
            System.out.println("\n");
        }
         */

        //invertedIndex.saveIndexToFile("index.txt");
    }
}
