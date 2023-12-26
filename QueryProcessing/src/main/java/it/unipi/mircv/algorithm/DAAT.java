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

    /**
     * Get the minimum DocID of the query's posting lists.
     * @param postingLists Array list of the posting lists of the query
     * @return The minimum DocID between the posting lists
     */
    private static int getMinimumDocId(ArrayList<PostingList> postingLists){
        int minDocId = (int) (CollectionInfo.getCollection_size() + 1);

        for (PostingList postingList : postingLists) {
            if(!postingList.getPl().isEmpty())
                minDocId = Math.min(postingList.getActualPosting().getDocId(), minDocId);
        }
        return minDocId;
    }

    /**
     * Execute the DAAT algorithm on the query's posting lists.
     * @param postingLists Array list of the posting lists of the query
     * @return The top k scored documents
     */
    public static PriorityQueue<DocumentScore> executeDAAT(ArrayList<PostingList> postingLists){
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(Flags.getNumDocs());

        int current = getMinimumDocId(postingLists);
        while (current < CollectionInfo.getCollection_size()) {
            double score = 0;

            // MAX DocID in the collection + 1
            int next = (int) (CollectionInfo.getCollection_size()+1);

            // For each posting list take one element
            for (PostingList list : postingLists) {
                String term = list.getTerm();

                // If the posting list taken is not null
                if(list.getActualPosting() != null) {

                    // Check if it is equal to the DocID to compute
                    if (list.getActualPosting().getDocId() == current) {

                        // Update the score of that document and advance the iterator
                        DictionaryElem dict = InvertedIndex.getDictionary().get(term);
                        score += Scorer.scoreDocument(list.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                        list.next();
                        if (list.getActualPosting() == null) {
                            continue;
                        }
                    }

                    // Take the new posting list to compute (the new minimum)
                    if (list.getActualPosting().getDocId() < next){
                        next = list.getActualPosting().getDocId();
                    }
                }
            }
            if (score != 0)
                result.offer(new DocumentScore(current, score));
            if (result.size() > Flags.getNumDocs())

                // Remove the document with the minimum score if beyond k
                result.poll();

            current = next;
        }
        return result;
    }
}
