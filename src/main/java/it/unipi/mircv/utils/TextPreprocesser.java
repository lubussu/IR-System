package it.unipi.mircv.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.tartarus.snowball.ext.EnglishStemmer;

public class TextPreprocesser {

    // File containing the stopwords
    public static List<String> stopwords_global;

    /**
     * Execute text preprocessing on the query.
     * @param str String containing the query to process
     * @return The processed query
     */
    public static String cleanText(String str){

        // Remove URLs
        str = str.replaceAll("https?://\\S+\\s?", " ");

        // Reduce to lower case
        str = str.toLowerCase();

        // Remove HTML tags
        str = str.replaceAll("<[^>]*>", "");

        // Remove punctuation
        str = str.replaceAll("\\p{Punct}", " ");

        // Remove extra whitespaces with a single one
        str = str.replaceAll("\\s+", " ");

        return str;
    }

    /**
     * Execute the tokenization of the query.
     * @param str String containing the query to process
     * @return An array list containing tokenized query
     */
    public static ArrayList<String> tokenizeLine(String str) {

        return Stream.of(str.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    /**
     * Execute the stemming of the query tokens.
     * @param tokens Array list containing the query tokens to process
     * @return An array list containing stemmed tokens
     */
    public static ArrayList<String> stemmingToken(ArrayList<String> tokens) {
        ArrayList<String> stemmedTokens = new ArrayList<>();

        // Use the Tartarus Snowball English Stemmer
        EnglishStemmer stemmer = new EnglishStemmer();

        for (String token : tokens) {
            stemmer.setCurrent(token);
            if (stemmer.stem()) {
                stemmedTokens.add(stemmer.getCurrent());
            } else {

                // If stemming fails, keep the original token
                stemmedTokens.add(token);
            }
        }

        return stemmedTokens;
    }

    /**
     * Execute stopword removal of the query tokens.
     * @param tokens Array list containing the query tokens to process
     * @return An array list containing stemmed tokens
     */
    public static ArrayList<String> removeStopwords(ArrayList<String> tokens) {
        tokens.removeAll(stopwords_global);
        return tokens;
    }

    /**
     * Execute the full pipeline for query preprocessing (Text Cleaning, Tokenization, Stopword Removal and Stemming).
     * @param line String containing the query to process
     * @return An array list containing preprocessed tokens
     */
    public static ArrayList<String> executeTextPreprocessing(String line) {
        TextPreprocesser.setStopwords_global("IndexConstruction/src/main/resources/stopwords.txt");

        ArrayList<String> tokens;

        line = cleanText(line);
        tokens = tokenizeLine(line);
        tokens = removeStopwords(tokens);
        //tokens = stemmingToken(tokens);

        return tokens;
    }

    public static List<String> getStopwords_global() {
        return stopwords_global;
    }

    public static void setStopwords_global(String path) {
        try {
            TextPreprocesser.stopwords_global= Files.readAllLines(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
