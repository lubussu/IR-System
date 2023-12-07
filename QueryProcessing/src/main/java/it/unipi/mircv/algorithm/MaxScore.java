package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.*;

import java.util.*;

public class MaxScore {

    private static int getMinimumDocId(ArrayList<PostingList> postingLists){
        int minDocId = (int) (CollectionInfo.getCollection_size() + 1);

        for (PostingList postingList : postingLists) {
            if(!postingList.getPl().isEmpty())
                minDocId = Math.min(postingList.getActualPosting().getDocId(), minDocId);
        }
        return minDocId;
    }

    /***
     *
     * @param postingLists lista di posting lists per i termini della query (stesso ordinamento di maxScores)
     * @param maxScores -> lista ordinata di maxScores per i termini della query
     * @return
     */
    public static PriorityQueue<DocumentScore> executeMaxScore(ArrayList<PostingList> postingLists, ArrayList<Double> maxScores) {
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(Flags.getNumDocs());

        /* Inizializzazione upper_bound */
        ArrayList<Double> ub = new ArrayList<>();
        ub.add(maxScores.get(0));
        for (int i = 1; i<postingLists.size(); i++)
            ub.add(ub.get(i-1)+maxScores.get(i));

        // Set initial threshold, pivot and the first docID to analyze */
        double threshold = 0;
        double score;
        int next;
        int pivot = 0;

        int current = getMinimumDocId(postingLists);
        double minScore = Double.MAX_VALUE;

        /* While there is at least an essential list and there are documents to process */
        while (pivot < postingLists.size() && current != -1){
            score = 0;
            next = (int) (CollectionInfo.getCollection_size()+1); //MAX docId in the collection + 1

            /* Liste essenziali */
            for (int i = pivot; i < postingLists.size(); i++){

                if (postingLists.get(i).getActualPosting() == null)
                    continue;

                String term = postingLists.get(i).getTerm();
                if (postingLists.get(i).getActualPosting().getDocId() == current){
                    Posting current_posting = postingLists.get(i).getActualPosting();
                    DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                    score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());

                    postingLists.get(i).next();
                    if (postingLists.get(i).getActualPosting() == null) {
                        current = -1;
                        continue;
                    }
                }

                if (postingLists.get(i).getActualPosting().getDocId() < next){
                    next = postingLists.get(i).getActualPosting().getDocId();
                }
            }

            /* Liste non essenziali */
            for (int i = pivot - 1; i > 0; i--) {

                if (postingLists.get(i).getActualPosting() == null)
                    continue;

                if (score + ub.get(i) <= threshold)
                    /* We are sure that the candidate docID cannot be in the final top k documents, and the remaining
                        posting lists can be skipped completely
                    */
                    break;

                /* Return -1 if the posting list doesn't have a docID grater or equal 'current' */
                postingLists.get(i).nextGEQ(current);

                if (postingLists.get(i).getActualPosting() == null) {
                    continue;
                }

                String term = postingLists.get(i).getTerm();
                if (postingLists.get(i).getActualPosting().getDocId() == current) {
                    Posting current_posting = postingLists.get(i).getActualPosting();
                    DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                    score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());
                }
            }

            /* Aggiornamento del pivot */
            if(result.offer(new DocumentScore(current, score))){
                minScore = Math.min(score, minScore);
                threshold = minScore;

                if (result.size() > Flags.getNumDocs()) {
                    result.poll();
                }

                while (pivot < postingLists.size() && ub.get(pivot) <= threshold){
                    pivot++;
                }
            }
            if (next == CollectionInfo.getCollection_size()+1)
                break;
            else
                current = next;
        }

        return result;
    }
}

