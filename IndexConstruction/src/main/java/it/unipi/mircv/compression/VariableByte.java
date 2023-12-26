package it.unipi.mircv.compression;

import java.util.ArrayList;

public class VariableByte {

    /**
     * Takes in input an array of integers and returns them in a compressed VariableBytes format.
     *
     * @param integers Array of integers to compress
     * @throws IllegalArgumentException Error while opening the file channel
     * @return ArrayList of bytes containing the new codification for the integers passed
     */
    public static byte[] fromIntegersToVariableBytes(ArrayList<Integer> integers) {
        ArrayList<Byte> encodedBytes = new ArrayList<>();

        for (int docId : integers) {
            do {
                // Takes the las 7 bits of the DocID and convert them in bytes
                byte b = (byte) (docId & 0x7F);
                // Move the 7 bits to the right
                docId >>= 7;

                if (docId != 0) {
                    // Set the most significant bit to 1
                    b |= 0x80;
                }

                encodedBytes.add(b);

            } while (docId != 0);
        }

        byte[] result = new byte[encodedBytes.size()];
        for (int i = 0; i < encodedBytes.size(); i++) {
            result[i] = encodedBytes.get(i);
        }
        return result;
    }

    /**
     * Takes in input an array of bytes and returns the decode integers from VariableBytes encoding.
     *
     * @param bytes Array of bytes to decompress
     * @throws IllegalArgumentException Error while opening the file channel
     * @return ArrayList containing decompressed integers
     */
    public static ArrayList<Integer> fromVariableBytesToIntegers(byte[] bytes, int length) {
        ArrayList<Integer> integers = new ArrayList<>();

        int currentIndex = 0;

        while (currentIndex < bytes.length && length > 0) {
            int result = 0;
            int shift = 0;

            while (true) {
                byte b = bytes[currentIndex];
                currentIndex++;
                result |= (b & 0x7F) << shift;
                shift += 7;

                if ((b & 0x80) == 0) {
                    break;
                }
            }
            integers.add(result);
            length --;
        }
        integers.add(0, currentIndex);
        return integers;
    }
}