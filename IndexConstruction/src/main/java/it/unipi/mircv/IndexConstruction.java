package it.unipi.mircv;

import it.unipi.mircv.utils.Flags;


public class IndexConstruction {
    public static final String PATH_TO_COLLECTION = "IndexConstruction/src/main/resources/collection.tar.gz";

    public static void main(String[] args) {

        String operation = "build"; // Imposta il valore predefinito come "build" se nessun argomento è passato

        if (args.length > 0) {
            if (args[0].equals("merge") || args[0].equals("read")) {
                operation = args[0];
            }
        }

        if (operation.equals("build")) {
            /* Costruzione indice da file*/
            InvertedIndex.buildIndexFromFile(PATH_TO_COLLECTION);
            InvertedIndex.mergeIndexes();
            InvertedIndex.buildCachePostingList();
            if(Flags.isTesting())
                InvertedIndex.test();
        } else if (operation.equals("merge")) {
            /* Merge index blocks from file*/
            InvertedIndex.mergeIndexes();
            InvertedIndex.buildCachePostingList();
            if(Flags.isTesting())
                InvertedIndex.test();
        } else if (operation.equals("read")) {
            /* read Merged Index from files*/
            InvertedIndex.readIndexFromFile();
        }

    }
}
