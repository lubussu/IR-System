package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.Scorer;

import java.util.*;


public class DAAT {

    public static PriorityQueue<DocumentScore> executeDAAT(ArrayList<PostingList> postingLists,
                                                           int k) {
//        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);
//        for (int docId = 0; docId < InvertedIndex.getDictionary().size(); docId++) {
//            double score = 0;
//            boolean present = true;
//            for (PostingList list : postingLists) {
//                String term = list.getTerm();
//                Posting current_posting = list.getActualPosting();
//                if(current_posting==null){
//                    list.next();
//                    current_posting = list.getActualPosting();
//                }
//                if(current_posting != null) {
//                    if (current_posting.getDocId() != docId && Flags.isQueryMode()) {
//                        present = false;
//                        continue;
//                    } else if (current_posting.getDocId() == docId) {
//                        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
//                        score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());
//                        list.next();
//                        if (list.getActualPosting() == null)
//                            break;
//                    }
//
//                    if ((Flags.isQueryMode() && present) || (!Flags.isQueryMode() && score != 0))
//                        result.offer(new DocumentScore(docId, score));
//                    if (result.size() > k)
//                        result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
//                }
//            }
//        }
//        return result;
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);
        ArrayList<Posting> actual_postings = new ArrayList<>(postingLists.size());
        int minDocId = Integer.MAX_VALUE;
        ArrayList<Boolean> finished = new ArrayList<>(postingLists.size()); // Creates a boolean array of the specified length
        for (int i = 0; i < postingLists.size(); i++) {
            finished.add(i, false); // Initialize each element to a specific value (e.g., true)
        }

        while(finished.contains(false)){

            for (int i = 0; i < postingLists.size(); i++) {
                if(finished.get(i))
                    continue;

                actual_postings.add(i, postingLists.get(i).getActualPosting());
                if(postingLists.get(i).getActualPosting().getDocId() < minDocId)
                    minDocId = postingLists.get(i).getActualPosting().getDocId();
            }

            double score = 0;

            for(int i = 0; i < postingLists.size(); i++){
                if(finished.get(i))
                    continue;
                if(actual_postings.get(i).getDocId() == minDocId){
                    DictionaryElem dict = InvertedIndex.getDictionary().get(postingLists.get(i).getTerm());
                    score += Scorer.scoreDocument(actual_postings.get(i), dict.getIdf(), Flags.isScoreMode());
                    postingLists.get(i).next();
                    if(postingLists.get(i).getActualPosting() == null)
                        finished.set(i, true);
                }
            }
            result.offer(new DocumentScore(minDocId, score));
            if (result.size() > k)
                result.poll();
            actual_postings.clear();
        }

        return result;
    }

}
