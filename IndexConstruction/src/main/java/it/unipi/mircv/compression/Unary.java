package it.unipi.mircv.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

public class Unary {

    public static byte[] fromIntToUnary(ArrayList<Integer> integers) {
        ArrayList<Byte> unaryBytes = new ArrayList<>();

        for (int integer : integers) {
            if (integer < 0) {
                throw new IllegalArgumentException("La codifica unary è valida solo per interi positivi.");
            }

            for (int i = 0; i < integer; i++) {
                unaryBytes.add((byte) '1');
            }
            unaryBytes.add((byte) '0');
        }

        byte[] result = new byte[unaryBytes.size()];
        for (int i = 0; i < unaryBytes.size(); i++) {
            result[i] = unaryBytes.get(i);
        }

        return result;
    }

    public static ArrayList<Integer> fromUnaryToInt(byte[] unaryBytes) {
        ArrayList<Integer> integers = new ArrayList<>();
        int count = 0;

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


