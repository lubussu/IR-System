package it.unipi.mircv.bean;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class CollectionInfo {
    private static long collection_size = 0;
    private static long collection_total_len = 0;

    public static void ToBinFile(FileChannel channel){
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(collection_size);
            buffer.putLong(collection_total_len);

            buffer.flip();
            // Write the buffer to the file
            channel.write(buffer);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void FromBinFile(FileChannel channel){
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            channel.read(buffer);
            buffer.flip();
            collection_size = buffer.getLong();
            collection_total_len = buffer.getLong();
            buffer.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getCollection_size() {
        return collection_size;
    }

    public static long getCollection_total_len() {
        return collection_total_len;
    }

    public static void setCollection_total_len(long collection_total_len) {
        CollectionInfo.collection_total_len = collection_total_len;
    }

    public static void setCollection_size(long collection_size) {
        CollectionInfo.collection_size = collection_size;
    }
}
