package it.unipi.mircv;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class InvertedIndex {
    private Map<String, Set<Integer>> index;
    private List<String> documents;

    public InvertedIndex() {
        index = new HashMap<>();
        documents = new ArrayList<>();
    }

    public String getDocument(int docId) {
        if (docId >= 0 && docId < documents.size()) {
            return documents.get(docId);
        }
        return null;
    }

    public Set<Integer> search(String query) {
        query = query.toLowerCase();
        return index.getOrDefault(query, Collections.emptySet());
    }

    public void buildIndexFromFile(String filePath) {
        ArrayList<String> tokens;
        int freq;
        int docid = 0;
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

                for (String term : tokens) {
                    freq = Collections.frequency(tokens, term);
                    if (!index.containsKey(term)) {
                        index.put(term, new HashSet<>());
                    }
                    index.get(term).add(docid);
                }

                if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED.\n");
                }

                tokens.clear();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void saveIndexToFile(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (String token : index.keySet()) {
                writer.write(token + ": " + index.get(token).toString() + "\n");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        String filePath = "src/main/resources/collection.tsv";

        InvertedIndex invertedIndex = new InvertedIndex();
        invertedIndex.buildIndexFromFile(filePath);


        String query = "geographic";
        Set<Integer> results = invertedIndex.search(query);

        if (!results.isEmpty()) {
            System.out.println("Documents containing '" + query + "':");
            for (int docIdResult : results) {
                System.out.println("Document " + docIdResult + ": " + invertedIndex.getDocument(docIdResult));
            }
        } else {
            System.out.println("No documents found for '" + query + "'.");
        }

        invertedIndex.saveIndexToFile("index.txt");
    }
}
