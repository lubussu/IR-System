package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.CollectionInfo;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.Scorer;

import java.util.*;


public class DAAT {

    private static int getMinimumDocId(ArrayList<PostingList> postingLists){
        int minDocId = (int) (CollectionInfo.getCollection_size() + 1);

        for (PostingList postingList : postingLists) {
            if(!postingList.getPl().isEmpty())
                minDocId = Math.min(postingList.getActualPosting().getDocId(), minDocId);
        }
        return minDocId;
    }

    public static PriorityQueue<DocumentScore> executeDAAT(ArrayList<PostingList> postingLists){
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(Flags.getNumDocs());

        int current = getMinimumDocId(postingLists);
        while (current < CollectionInfo.getCollection_size()) {
            double score = 0;
            int next = (int) (CollectionInfo.getCollection_size()+1); //MAX docId in the collection + 1
            for (PostingList list : postingLists) {
                String term = list.getTerm();

                if(list.getActualPosting() != null) {
                    if (list.getActualPosting().getDocId() == current) {
                        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                        score += Scorer.scoreDocument(list.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                        list.next();
                        if (list.getActualPosting() == null) {
                            continue;
                        }
                    }
                    if (list.getActualPosting().getDocId() < next){
                        next = list.getActualPosting().getDocId();
                    }
                }
            }
            if (score != 0)
                result.offer(new DocumentScore(current, score));
            if (result.size() > Flags.getNumDocs())
                result.poll(); // Rimuovi il documento con il punteggio più basso se supera k

            current = next;
        }
        return result;
    }
}
