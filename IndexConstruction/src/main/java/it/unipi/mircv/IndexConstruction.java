package it.unipi.mircv;

import it.unipi.mircv.utils.Flags;


public class IndexConstruction {

    // Path to the document collection archive
    public static final String PATH_TO_COLLECTION = "IndexConstruction/src/main/resources/collection.tar.gz";

    public static void main(String[] args) {

        // Set the default value to "build" if nothing is passed
        String operation = "build";

        // Check the operation to do
        if (args.length > 0) {
            if (args[0].equals("merge") || args[0].equals("read")) {
                operation = args[0];
            }
        }

        if (operation.equals("build")) {

            // Build the temporary index on disk
            InvertedIndex.buildIndexFromFile(PATH_TO_COLLECTION);

            // Merge the temporary files into the final index
            InvertedIndex.mergeIndexes();

            // Build the cache taking the most common terms
            InvertedIndex.buildCachePostingList();

            // Execute testing of the functions
            if(Flags.isTesting())
                InvertedIndex.test();
        } else if (operation.equals("merge")) {

            // Merge the temporary files into the final index
            InvertedIndex.mergeIndexes();

            // Build the cache taking the most common terms
            InvertedIndex.buildCachePostingList();

            // Execute testing of the functions
            if(Flags.isTesting())
                InvertedIndex.test();
        } else if (operation.equals("read")) {
            // Read the final index from file
            InvertedIndex.readIndexFromFile();
        }

    }
}
