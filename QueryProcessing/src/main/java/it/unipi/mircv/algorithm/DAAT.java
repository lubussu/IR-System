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
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(k);
        for (int docId = 0; docId < InvertedIndex.getDictionary().size(); docId++) {
            double score = 0;
            boolean present = true;
            for (PostingList list : postingLists) {
                String term = list.getTerm();
                Posting current_posting = list.getActualPosting();
                if(current_posting==null){
                    list.next();
                    current_posting = list.getActualPosting();
                }
                if(current_posting != null) {
                    if (current_posting.getDocId() != docId && Flags.isQueryMode()) {
                        present = false;
                        continue;
                    } else if (current_posting.getDocId() == docId) {
                        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                        score += Scorer.scoreDocument(current_posting, dict.getIdf(), Flags.isScoreMode());
                        list.next();
                        if (list.getActualPosting() == null)
                            break;
                    }

                    if ((Flags.isQueryMode() && present) || (!Flags.isQueryMode() && score != 0))
                        result.offer(new DocumentScore(docId, score));
                    if (result.size() > k)
                        result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
                }
            }
        }
        return result;

    }
}
