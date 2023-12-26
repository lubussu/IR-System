package it.unipi.mircv.compression;

import java.io.IOException;
import java.util.ArrayList;

public class Unary {

    /**
     * Takes in input an array of integers and returns them in a compressed Unary format.
     *
     * @param integers Array of integers to compress
     * @throws IllegalArgumentException Error while opening the file channel
     * @return ArrayList of bytes containing the new codification for the integers passed
     */
    public static byte[] fromIntToUnary(ArrayList<Integer> integers) {

        // Initialize the array list to save the new codification
        ArrayList<Byte> unaryBytes = new ArrayList<>();

        // Iterate all the integers passed
        for (int integer : integers) {
            if (integer < 0) {
                throw new IllegalArgumentException("La codifica unary è valida solo per interi positivi.");
            }

            // Add as many 1 as the integer value then add 0
            for (int i = 0; i < integer; i++) {
                unaryBytes.add((byte) '1');
            }
            unaryBytes.add((byte) '0');
        }

        // Save the new coded integer
        byte[] result = new byte[unaryBytes.size()];
        for (int i = 0; i < unaryBytes.size(); i++) {
            result[i] = unaryBytes.get(i);
        }

        return result;
    }

    /**
     * Takes in input an array of bytes and returns the decode integers from Unary encoding.
     *
     * @param unaryBytes Array of bytes to decompress
     * @throws IllegalArgumentException Error while opening the file channel
     * @return ArrayList containing decompressed integers
     */
    public static ArrayList<Integer> fromUnaryToInt(byte[] unaryBytes) {
        ArrayList<Integer> integers = new ArrayList<>();
        int count = 0;

        // For each byte count the number of 1 till 0 is reached
        for (byte b : unaryBytes) {
            if (b == '1') {
                count++;
            } else if (b == '0') {
                integers.add(count);
                count = 0;
            } else {
                throw new IllegalArgumentException("La sequenza di byte contiene caratteri non validi.");
            }
        }
        return integers;
    }
}


