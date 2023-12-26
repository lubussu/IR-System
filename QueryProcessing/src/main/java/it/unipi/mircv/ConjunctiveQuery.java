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

    /**
     * Computes the preprocessing of the query needed to start the conjunctive query execution.
     * @param queryTerms Array list of the query's terms
     * @return An array list of document scores ordered in decreasing order
     */
    public static PriorityQueue<DocumentScore> executeQueryProcesser(ArrayList<String> queryTerms){

        // Initialize the priority queue with the query's term dictionary element in increasing order of document frequency
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

        // Store in memory the first posting list of the query's terms, the one with the smallest list
        DictionaryElem first_dict = term_df_inc.poll();
        String path = IOUtils.PATH_TO_FINAL_BLOCKS+"/indexMerged" + first_dict.getBlock_number();
        FileChannel channel = IOUtils.getFileChannel(path, "read");
        PostingList pl = IOUtils.readPlFromFile(channel, first_dict.getOffset_block_pl(), first_dict.getTerm());
        IOUtils.closeChannel(channel);
        postingLists.add(pl);
        while(!term_df_inc.isEmpty()){
            DictionaryElem dict = term_df_inc.poll();

            // Get the posting list offset in memory
            int offset = dict.getOffset_posting_lists();

            // If the posting list is in memory take it directly
            if(offset!=-1 && InvertedIndex.getPosting_lists().size() > offset
                    && InvertedIndex.getPosting_lists().get(offset).getTerm().equals(dict.getTerm())) {

                pl = InvertedIndex.getPosting_lists().get(offset);
            } else {

                if (Flags.isSkipping()) {

                    // If it is not in memory and there are skipping lists initialize empty posting lists (don't need to
                    // load in memory all the posting lists)
                    pl = new PostingList(dict.getTerm());
                } else {

                    // Read from file all the remaining posting lists
                    path = IOUtils.PATH_TO_FINAL_BLOCKS + "/indexMerged" + dict.getBlock_number();
                    channel = IOUtils.getFileChannel(path, "read");
                    pl = IOUtils.readPlFromFile(channel, dict.getOffset_block_pl(), dict.getTerm());
                }
            }
            pl.initList();
            postingLists.add(pl);
        }

        // Execute the conjunctive query
        PriorityQueue<DocumentScore> retrievedDocs = ConjunctiveQuery.executeQuery(postingLists);
        postingLists.clear();
        return retrievedDocs;
    }

    /**
     * Execute the conjunctive query algorithm.
     * @param postingLists Array list of the query's terms, in increasing length order
     * @return An array list of document scores ordered in decreasing order
     */
    public static PriorityQueue<DocumentScore> executeQuery(ArrayList<PostingList> postingLists){
        PriorityQueue<DocumentScore> result = new PriorityQueue<>(Flags.getNumDocs());

        // Get posting list of the first term in the list (the one with the smallest posting list)
        PostingList first_pl = postingLists.get(0);
        first_pl.initList();

        // While the first as elements
        while(first_pl.getActualPosting() != null){

            // Get the posting pointed by the iterator
            Posting post = first_pl.getActualPosting();
            DictionaryElem dict = InvertedIndex.getDictionary().get(first_pl.getTerm());

            // Compute the partial score
            double score = Scorer.scoreDocument(post, dict.getIdf(), Flags.isScoreMode());
            boolean present = true;

            // Check the other posting lists for the current DocID to compute
            for(int i = 1; i<postingLists.size(); i++){
                PostingList current_pl = postingLists.get(i);
                dict = InvertedIndex.getDictionary().get(current_pl.getTerm());

                // The nextGEQ() will jump to the next DocID greater or equal to the one pointed by the first posting list iterator
                // If there are skipping lists, it will read from file only the block that may contain the current DocID
                // currentDocID <= MaxDocIDblock < nextMaxDocIDblock)

                //Else it will search in the posting list in memory the next DocID greater or equal of the current DocID
                current_pl.nextGEQ(post.getDocId());
                Posting actualPosting = current_pl.getActualPosting();
                if(actualPosting != null && actualPosting.getDocId() == post.getDocId()){

                    // If the current DocID is found in another posting list compute the partial score
                    score += Scorer.scoreDocument(current_pl.getActualPosting(), dict.getIdf(), Flags.isScoreMode());
                }else{

                    // Else it is not present and advance the first posting list iterator and discard this DocID (ALL OR NOTHING)
                    present = false;
                    first_pl.next();
                    break;
                }
            }
            if (present) {
                result.offer(new DocumentScore(post.getDocId(), score));
            }
            if (result.size() > Flags.getNumDocs()) {
                result.poll();
            }
            first_pl.next();
        }
        return result;
    }
}
