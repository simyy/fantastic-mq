package com.github.fantasticlab.mq.core.broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelTest {

    public static void main(String[] args) throws IOException {

        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES * 2);
        offsetBuffer.putLong(0l);
        offsetBuffer.putLong(Long.BYTES, 44l);
        FileChannel fileChannel = new RandomAccessFile(new File("test1"), "rwd").getChannel();
        fileChannel.write(ByteBuffer.wrap(offsetBuffer.array()), 0);

        ByteBuffer offsetBuffer1 = ByteBuffer.allocate(Long.BYTES * 2);
        offsetBuffer1.putLong(44l);
        offsetBuffer1.putLong(Long.BYTES, 43l);
        fileChannel.write(ByteBuffer.wrap(offsetBuffer1.array()), Long.BYTES * 2);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        fileChannel.read(buffer);
        System.out.println(buffer.getLong(0));
        System.out.println(buffer.getLong(Long.BYTES));

        ByteBuffer buffer1 = ByteBuffer.allocate(Long.BYTES * 2);
        fileChannel.read(buffer1, Long.BYTES * 2);
        System.out.println(buffer1.getLong(0));
        System.out.println(buffer1.getLong(Long.BYTES));

    }

}
