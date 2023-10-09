package it.unipi.mircv.compression;

import java.util.ArrayList;

public class VariableByte {

    public static byte[] fromIntegersToVariableBytes(ArrayList<Integer> integers) {
        ArrayList<Byte> encodedBytes = new ArrayList<>();

        for (int docId : integers) {
            do {
                byte b = (byte) (docId & 0x7F); // Ultimi 7 bit di docId e converti in byte
                docId >>= 7; // Sposta i 7 bit verso destra

                if (docId != 0) {
                    // Imposta il bit più significativo di b a 1
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

    public static ArrayList<Integer> fromVariableBytesToIntegers(byte[] bytes, int lenght) {
        ArrayList<Integer> integers = new ArrayList<>();

        int currentIndex = 0;

        while (currentIndex < bytes.length && lenght > 0) {
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
            lenght --;
        }
        integers.add(0, currentIndex);
        return integers;
    }
}