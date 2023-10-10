package it.unipi.mircv;

public class IndexConstruction {
    public static void main(String[] args) {
        String filePath = "IndexConstruction/src/main/resources/collection.tsv";

        InvertedIndex invertedIndex = new InvertedIndex(true);
        //invertedIndex.buildIndexFromFile(filePath);
        //invertedIndex.writeTermList();

        invertedIndex.clearIndexMem();
        invertedIndex.clearDictionaryMem();
        invertedIndex.readIndexFromFile();

    }
}
