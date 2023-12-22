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
        IOUtils.closeChannel(channel);
        postingLists.add(pl);
        while(!term_df_inc.isEmpty()){
            DictionaryElem dict = term_df_inc.poll();
            int offset = dict.getOffset_posting_lists();
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset
                    && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(dict.getTerm())) {

                pl = InvertedIndex.getPosting_lists().get(offset);
            } else {
                if (Flags.isSkipping()) {
                    pl = new PostingList(dict.getTerm());
                } else {
                    path = IOUtils.PATH_TO_FINAL_BLOCKS + "/indexMerged" + dict.getBlock_number();
                    channel = IOUtils.getFileChannel(path, "read");
                    pl = IOUtils.readPlFromFile(channel, dict.getOffset_block_pl(), dict.getTerm());
                }
            }
            pl.initList();
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
        first_pl.initList();
        while(first_pl.getActualPosting() != null){
            Posting post = first_pl.getActualPosting();
            DictionaryElem dict = InvertedIndex.getDictionary().get(first_pl.getTerm());
            double score = Scorer.scoreDocument(post, dict.getIdf(), Flags.isScoreMode());
            boolean present = true;

            for(int i = 1; i<postingLists.size(); i++){
                PostingList current_pl = postingLists.get(i);
                dict = InvertedIndex.getDictionary().get(current_pl.getTerm());
                current_pl.nextGEQ(post.getDocId());
                Posting actualPosting = current_pl.getActualPosting();
                if(actualPosting != null && actualPosting.getDocId() == post.getDocId()){
                    score += Scorer.scoreDocument(current_pl.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                }else{
                    present = false;
                    first_pl.next();
                    break;
                }
            }
            if (present) {
                result.offer(new DocumentScore(post.getDocId(), score));
            }
            if (result.size() > Flags.getNumDocs()) {
                result.poll(); // Rimuovi il documento con il punteggio più basso se supera k
            }
            first_pl.next();
        }
        return result;
    }
}
