package it.unipi.mircv;

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
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.EnglishStemmer;

public class TextPreprocesser {

    public static List<String> stopwords_global;

    private static String removeUnicodeChars(String s) {

        String str;
        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);

        str = new String(strBytes, StandardCharsets.UTF_8);

        Pattern unicodeOutliers = Pattern.compile("[^\\x00-\\x7F]",
                Pattern.UNICODE_CASE | Pattern.CANON_EQ
                        | Pattern.CASE_INSENSITIVE);

        Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(str);
        str = unicodeOutlierMatcher.replaceAll(" ");

        return str;
    }

    public static String cleanText(String str){

        /* Remove URLs */
        str = str.replaceAll("https?://\\S+\\s?", " ");

        /* Reduce to lower case */
        str = str.toLowerCase();

        /* Remove HTML tags */
        str = str.replaceAll("<[^>]*>", "");

        /* Remove punctuation */
        str = str.replaceAll("\\p{Punct}", " ");

        /* Remove Unicode chars */
        //str = removeUnicodeChars(str);   rivedi metodo

        /* Remove extra whitespaces with a single one */
        str = str.replaceAll("\\s+", " ");

        return str;
    }

    public static ArrayList<String> tokenizeLine(String str) {

        return Stream.of(str.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    public static ArrayList<String> stemmingToken(ArrayList<String> tokens) {
        ArrayList<String> stemmedTokens = new ArrayList<>();
        EnglishStemmer stemmer = new EnglishStemmer();

        for (String token : tokens) {
            stemmer.setCurrent(token);
            if (stemmer.stem()) {
                stemmedTokens.add(stemmer.getCurrent());
            } else {
                stemmedTokens.add(token); // If stemming fails, keep the original token
            }
        }

        return stemmedTokens;
    }


    public static ArrayList<String> removeStopwords(ArrayList<String> tokens) {
        tokens.removeAll(stopwords_global);
        return tokens;
    }

    public static ArrayList<String> executeTextPreprocessing(String line) {
        TextPreprocesser.setStopwords_global("IndexConstruction/src/main/resources/stopwords.txt");

        ArrayList<String> tokens;

        line = cleanText(line);
        tokens = tokenizeLine(line);
        tokens = removeStopwords(tokens);
        tokens = stemmingToken(tokens);

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
