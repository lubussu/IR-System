package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.Scorer;

import java.util.*;


public class DAAT {

    private static int getMinimumDocId(ArrayList<PostingList> postingLists){
        int minDocId = (int) (CollectionInfo.getCollection_size() + 1);

        for (PostingList postingList : postingLists) {
            if(postingList.getActualPosting()==null){
                postingList.next();
            }
            minDocId = Math.min(postingList.getActualPosting().getDocId(), minDocId);
        }
        return minDocId;
    }

    public static PriorityQueue<DocumentScore> executeDAAT(ArrayList<PostingList> postingLists,
                                                            int k){
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);

        int current = getMinimumDocId(postingLists);
        while (current < CollectionInfo.getCollection_size()) {
            double score = 0;
            int next = (int) (CollectionInfo.getCollection_size()+1); //MAX docId in the collection + 1
            boolean present = true;
            for (PostingList list : postingLists) {
                String term = list.getTerm();
                Posting current_posting = list.getActualPosting();

                if(current_posting != null) {
                    if (current_posting.getDocId() != current && Flags.isQueryMode()) {
                        present = false;
                        continue;
                    } else if (current_posting.getDocId() == current) {
                        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                        score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());
                        list.next();
                        if (list.getActualPosting() == null) {
                            break;
                        }
                        if (list.getActualPosting().getDocId() < next){
                            next = list.getActualPosting().getDocId();
                        }
                    }

                    if ((Flags.isQueryMode() && present) || (!Flags.isQueryMode() && score != 0))
                        result.offer(new DocumentScore(current, score));
                    if (result.size() > k)
                        result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
                }
            }
            current = next;
        }
        return result;
    }

//    public static PriorityQueue<DocumentScore> executeDAATDisjunctive(ArrayList<PostingList> postingLists,
//                                                           int k) {
//        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);
//        ArrayList<Posting> actual_postings = new ArrayList<>(postingLists.size());
//        int minDocId = Integer.MAX_VALUE;
//        ArrayList<Boolean> finished = new ArrayList<>(postingLists.size()); // Creates a boolean array of the specified length
//        for (int i = 0; i < postingLists.size(); i++) {
//            finished.add(i, false);
//            actual_postings.add(i, postingLists.get(i).getActualPosting());
//            if(postingLists.get(i).getActualPosting().getDocId() < minDocId)
//                minDocId = postingLists.get(i).getActualPosting().getDocId();
//        }
//
//        while(finished.contains(false)){
//
//            double score = 0;
//            int temp_min = Integer.MAX_VALUE;
//
//            for(int i = 0; i < postingLists.size(); i++){
//                if(finished.get(i))
//                    continue;
//
//                if(actual_postings.get(i).getDocId() == minDocId){
//                    DictionaryElem dict = InvertedIndex.getDictionary().get(postingLists.get(i).getTerm());
//                    score += Scorer.scoreDocument(actual_postings.get(i), dict.getIdf(), Flags.isScoreMode());
//                    postingLists.get(i).next();
//                    actual_postings.set(i, postingLists.get(i).getActualPosting());
//                    if(postingLists.get(i).getActualPosting() == null) {
//                        finished.set(i, true);
//                        continue;
//                    }
//                }
//                if(postingLists.get(i).getActualPosting().getDocId() < temp_min)
//                    temp_min = postingLists.get(i).getActualPosting().getDocId();
//            }
//
//            result.offer(new DocumentScore(minDocId, score));
//            minDocId = temp_min;
//            if (result.size() > k)
//                result.poll();
//        }
//
//        return result;
//    }

}
