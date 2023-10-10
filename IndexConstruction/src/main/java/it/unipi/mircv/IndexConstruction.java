package it.unipi.mircv;

public class IndexConstruction {
    public static void main(String[] args) {
        String filePath = "IndexConstruction/src/main/resources/collection.tsv";

        InvertedIndex invertedIndex = new InvertedIndex(true);
        //invertedIndex.buildIndexFromFile(filePath);
        //invertedIndex.writeTermList();

        invertedIndex.readIndexFromFile();


//        String query = "intrabuilding";
//        ArrayList<Integer> results = invertedIndex.search(query);
//
//        if (!results.isEmpty()) {
//            System.out.println("Documents containing '" + query + "':");
//            for (int doc : results) {
//                System.out.println("Document " + doc + ": ");
//            }
//        } else {
//            System.out.println("No documents found for '" + query + "'.");
//        }

    }
}
