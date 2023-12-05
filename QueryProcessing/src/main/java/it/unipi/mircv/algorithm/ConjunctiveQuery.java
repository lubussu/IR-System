package it.unipi.mircv.algorithm;

import it.unipi.mircv.InvertedIndex;
import it.unipi.mircv.bean.DictionaryElem;
import it.unipi.mircv.bean.Posting;
import it.unipi.mircv.bean.PostingList;
import it.unipi.mircv.bean.SkipList;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.Scorer;

import java.util.ArrayList;
import java.util.PriorityQueue;

public class ConjunctiveQuery {
    public static PriorityQueue<DocumentScore> executeConjunctiveQuery(ArrayList<PostingList> postingLists){

        PriorityQueue<DocumentScore> result = new PriorityQueue<>(Flags.getNumDocs());
        // Get the skipping lists of the terms in the query
        PostingList first_pl = postingLists.get(0);

        for(Posting post : first_pl.getPl()){
            DictionaryElem dict = InvertedIndex.getDictionary().get(first_pl.getTerm());
            double score = Scorer.scoreDocument(post, dict.getIdf(), Flags.isScoreMode());
            boolean present = true;
            for(int i = 1; i<postingLists.size(); i++){
                dict = InvertedIndex.getDictionary().get(postingLists.get(i).getTerm());
                PostingList current_pl = postingLists.get(i);
                current_pl.nextGEQ(post.getDocId());
                if(current_pl.getActualPosting() != null && current_pl.getActualPosting().getDocId() == post.getDocId()){
                    score += Scorer.scoreDocument(current_pl.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                }else{
                    present = false;
                    break;
                }
            }
            if(present && score != 0){
                result.offer(new DocumentScore(post.getDocId(), score));
            }if (result.size() > Flags.getNumDocs()) {
                result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
            }
        }

        return result;
    }
}
