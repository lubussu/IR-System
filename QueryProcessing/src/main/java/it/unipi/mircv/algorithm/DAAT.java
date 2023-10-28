package it.unipi.mircv.algorithm;

import it.unipi.mircv.IndexConstruction;
import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.Utils.IOUtils;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.Flags;

import java.nio.channels.FileChannel;
import java.util.*;


public class DAAT {

    public static ArrayList<Integer> retrieveDocuments(ArrayList<String> queryTerms,
                                                       int k) {
        ArrayList<PostingList> postingLists = new ArrayList<>();
        // Ottenere le posting list dei termini nella query
        for (String term : queryTerms) {
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);
            PostingList termPL;
            int offset = dict.getOffset_posting_lists();
            if(InvertedIndex.getPosting_lists().size() >= offset && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(term)) {
                termPL = InvertedIndex.getPosting_lists().get(offset);
            }else{
                String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + dict.getBlock_number();
                FileChannel channel = IOUtils.getFileChannel(path, "read");
                termPL = IOUtils.readPlFromFile(channel, dict.getOffset_block(), term);
            }
            postingLists.add(termPL);
        }

        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);

        for (int docId = 0; docId < InvertedIndex.getDictionary().size(); docId++) {
            double score = 0;
            boolean present = true;
            for (PostingList list : postingLists) {
                String term = list.getTerm();
                Posting current_posting = list.getPl().get(0);
                if(current_posting.getDocId() != docId &&  Flags.isQueryMode()){
                    present = false;
                    continue;
                }
                else if (current_posting.getDocId() == docId) {
                    DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                    score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());
                    list.getPl().remove(0);
                }

                if((Flags.isQueryMode() && present)|| (!Flags.isQueryMode() && score!=0))
                    result.offer(new DocumentScore(docId, score));
                if (result.size() > k)
                    result.poll(); // Rimuovi il documento con il punteggio più basso se supera k

            }
        }

        ArrayList<Integer> topKResults = new ArrayList<>();
        while(!result.isEmpty()){
            DocumentScore ds = result.poll();
            System.out.println(ds.getDocId()+": " + ds.getScore());
            topKResults.add(ds.getDocId());
        }

        return topKResults;
    }

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

        public int getDocId() {
            return docId;
        }

        public void setDocId(int docId) {
            this.docId = docId;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }
    }

    public static void main(String[] args) {
        IndexConstruction.main(new String[]{"merge"});

        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("vax");
        tokens.add("vaulty");

        retrieveDocuments(tokens, 5);
    }
}
