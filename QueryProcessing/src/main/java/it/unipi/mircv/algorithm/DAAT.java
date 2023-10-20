package it.unipi.mircv.algorithm;

import it.unipi.mircv.IndexConstruction;
import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import lombok.Getter;
import lombok.Setter;

import javax.print.Doc;
import java.util.*;

import static java.lang.Math.log;

public class DAAT {

    public static ArrayList<Integer> retrieveDocuments(ArrayList<String> queryTerms, InvertedIndex index, int k) {
        ArrayList<PostingList> postingLists = new ArrayList<>();

        // Ottenere le posting list dei termini nella query
        for (String term : queryTerms) {
            DictionaryElem dict = index.getDictionary().get(term);
            PostingList termPL = index.getPosting_lists().get(dict.getOffset_posting_lists());
            postingLists.add(termPL);
        }

        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);

        for (int docId = 0; docId < index.getDictionary().size(); docId++) {
            double score = 0;

            for (PostingList list : postingLists) {
                String term = list.getTerm();
                Posting current_posting = list.getPl().get(0);
                if (current_posting.getDocId() == docId) {
                    DictionaryElem dict = index.getDictionary().get(term);
                    score += Scorer.scoreDocument(current_posting, dict.getIdf(), "idf");
                    list.getPl().remove(0);

                    result.offer(new DocumentScore(docId, score));
                    if (result.size() > k)
                        result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
                }
            }
        }

        ArrayList<Integer> topKResults = new ArrayList<>();
        while(!result.isEmpty()){
            DocumentScore ds = result.poll();
            //System.out.println(ds.getDocId()+": " + ds.getScore());
            topKResults.add(ds.getDocId());
        }

        return topKResults;
    }

    @Getter
    @Setter
    static class DocumentScore implements Comparable<DocumentScore> {
        private int docId;
        private double score;

        public DocumentScore(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }

        @Override
        public int compareTo(DocumentScore other) {
            return Double.compare(other.score, this.score);
        }
    }

    public static void main(String[] args) {
        InvertedIndex index = IndexConstruction.main(new String[]{"merge"});

        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("vax");
        tokens.add("vaulty");

        retrieveDocuments(tokens, index, 5);
    }

}
