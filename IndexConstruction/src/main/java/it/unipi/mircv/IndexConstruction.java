package it.unipi.mircv;

import it.unipi.mircv.bean.DictionaryElem;

public class IndexConstruction {
    public static final String PATH_TO_COLLECTION = "IndexConstruction/src/main/resources/collection.tsv";

    public static void main(String[] args) {

        String operation = "build"; // Imposta il valore predefinito come "build" se nessun argomento è passato

        if (args.length > 0) {
            if (args[0].equals("merge") || args[0].equals("read")) {
                operation = args[0];
            }
        }

        switch (operation) {
            case "build":
                /* Costruzione indice da file*/
                InvertedIndex.buildIndexFromFile(PATH_TO_COLLECTION);
                InvertedIndex.mergeIndexes();
                InvertedIndex.buildCachePostingList();
                break;
            case "merge":
                /* Merge index blocks from file*/
                InvertedIndex.mergeIndexes();
                InvertedIndex.buildCachePostingList();
                break;
            case "read":
                /* read Merged Index from files*/
                InvertedIndex.readIndexFromFile();
                break;
        }

    }
}
