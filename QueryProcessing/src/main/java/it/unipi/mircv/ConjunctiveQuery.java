package it.unipi.mircv;

import it.unipi.mircv.bean.*;
import it.unipi.mircv.utils.DocumentScore;
import it.unipi.mircv.utils.Flags;
import it.unipi.mircv.utils.IOUtils;
import it.unipi.mircv.utils.Scorer;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class ConjunctiveQuery {
    private static ArrayList<PostingList> postingLists = new ArrayList<>();

    public static PriorityQueue<DocumentScore> executeQueryProcesser(ArrayList<String> queryTerms){
        PriorityQueue<DictionaryElem> term_df_inc = new PriorityQueue<>(queryTerms.size(), ((a, b) -> a.compareTo(b)));
        for (String term: queryTerms){
            DictionaryElem dict = InvertedIndex.getDictionary().get(term);
            if(dict == null){
                break;
            }
            term_df_inc.add(dict);
        }

        if(term_df_inc.size() != queryTerms.size()) {
            System.out.println("(ERROR) No documents found\n");
            postingLists.clear();
            return new PriorityQueue<>();
        }

        DictionaryElem first_dict = term_df_inc.poll();
        String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + first_dict.getBlock_number();
        FileChannel channel = IOUtils.getFileChannel(path, "read");
        PostingList pl = IOUtils.readPlFromFile(channel, first_dict.getOffset_block_pl(), first_dict.getTerm());
        postingLists.add(pl);
        while(!term_df_inc.isEmpty()){
            pl = new PostingList(term_df_inc.poll().getTerm());
            postingLists.add(pl);
        }

        PriorityQueue<DocumentScore> retrievedDocs = ConjunctiveQuery.executeQuery(postingLists);
        postingLists.clear();
        return retrievedDocs;
    }

    public static PriorityQueue<DocumentScore> executeQuery(ArrayList<PostingList> postingLists){
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
                Posting actualPosting = current_pl.getActualPosting();
                if(actualPosting != null && actualPosting.getDocId() == post.getDocId()){
                    score += Scorer.scoreDocument(current_pl.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                }else{
                    present = false;
                    break;
                }
            }
            if (present) {
                result.offer(new DocumentScore(post.getDocId(), score));
            }
            if (result.size() > Flags.getNumDocs()) {
                result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
            }
        }
        return result;
    }
}
