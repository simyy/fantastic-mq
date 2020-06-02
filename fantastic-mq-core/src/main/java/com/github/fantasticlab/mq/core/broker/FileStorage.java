package com.github.fantasticlab.mq.core.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.TreeMap;

@Slf4j
public class FileStorage implements Persistence {

    private String topic;
    private int queue;

    private String DB_DATA = "db.data";
    private String DB_INDEX = "db.index";
    private String DB_OFFSET = "db.offset";

    private FileChannel dbChannel;
    private FileChannel indexChannel;
    private FileChannel latestOffsetChannel;
    private long nextOffsetOfData;
    private long nextOffsetOfIndex;

    private TreeMap index = new TreeMap();



    public FileStorage(String topic, int queue) {
        this.topic = topic;
        this.queue = queue;
        try {
            this.dbChannel = new RandomAccessFile(new File(topic + "-" + queue + "-" + DB_DATA), "rwd").getChannel();
            this.indexChannel = new RandomAccessFile(new File(topic + "-" + queue + "-" + DB_INDEX), "rwd").getChannel();
            this.latestOffsetChannel = new RandomAccessFile(new File(topic + "-" + queue  + "-" + DB_OFFSET), "rwd").getChannel();
            this.nextOffsetOfData = getLatestOffsetOfData();
            this.nextOffsetOfIndex = getLatestOffsetOfIndex();
        } catch (FileNotFoundException e) {
            log.error("FileStorage Init DB File error", e);
            throw new StorageException(e);
        }
    }

    private long getLatestOffsetOfData() {
        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        offsetBuffer.limit();
        try {
            latestOffsetChannel.read(offsetBuffer, 0);
            return offsetBuffer.getLong();
        } catch (BufferUnderflowException e) {
            return 0;
        } catch (IOException e) {
            return 0;
        }
    }

    private long getLatestOffsetOfIndex() {
        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        offsetBuffer.limit();
        try {
            latestOffsetChannel.read(offsetBuffer, Long.BYTES);
            return offsetBuffer.getLong();
        } catch (BufferUnderflowException e) {
            return 0;
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public Position writeMsg(Message msg) {
        return null;
    }

    @Override
    public Position writeMsg2Disk(Message msg) {
        try {

            ObjectMapper mapper = new ObjectMapper();
            byte[] bytesOfData = mapper.writeValueAsBytes(msg);
            long offsetOfData = nextOffsetOfData;
            int lengthOfData = bytesOfData.length;
            long offsetOfIndex = nextOffsetOfIndex;

            // Write NextOffset Into DB
            nextOffsetOfData += lengthOfData;
            nextOffsetOfIndex +=  Long.BYTES;
            ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            offsetBuffer.putLong(nextOffsetOfData);
            offsetBuffer.putLong(Long.BYTES, nextOffsetOfIndex);
            this.latestOffsetChannel.write(ByteBuffer.wrap(offsetBuffer.array()), 0);

            // Write Msg Into DB
            this.dbChannel.write(ByteBuffer.wrap((bytesOfData)), offsetOfData);

            // Write FileIndex Into DB
            index.put(lengthOfData, lengthOfData);
            ByteBuffer indexBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            indexBuffer.putLong(offsetOfData);
            indexBuffer.putLong(Long.BYTES, lengthOfData);
            this.indexChannel.write(indexBuffer, offsetOfIndex);

            Position position = new Position();
            position.setOffset(offsetOfData);
            position.setLength(lengthOfData);
            return position;
        } catch (IOException e) {
            log.error("FileStorage writeMsg2Disk error", e);
            throw new StorageException(e);
        }
    }

    @Override
    public boolean flushDisk(int milliseconds) {
        return false;
    }

    @Override
    public TreeMap loadIndex() {
        if (index.isEmpty()) {
            long start = 0;
            ByteBuffer indexBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            try {
                while (true) {
                    int rs = indexChannel.read(indexBuffer, start);
                    if (rs == 0) {
                        break;
                    }
                    start += Long.BYTES * 2;

                    long offset = indexBuffer.getLong();
                    long length = indexBuffer.getLong(Long.BYTES);
                    index.put(offset, length);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return index;
    }

    @Override
    public Message loadMsg(Position position) {
        try {
            if (position == null) {
                throw new StorageException("Position is Null");
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) position.getLength());
            dbChannel.read(buffer, position.getOffset());
            byte[] bytes = buffer.array();
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(bytes, Message.class);
        } catch (IOException e) {
            log.error("FileStorage loadMsg error", e);
            throw new StorageException(e);
        }
    }

    @Override
    public List<Message> loadMsg(List<Position> positions) {
        return null;
    }


    public static void main(String[] args) {
        Message msg1 = new Message();
        msg1.setTopic("topic1");
        msg1.setBody("body1");

        Message msg2 = new Message();
        msg2.setTopic("topic1");
        msg2.setBody("body2");
//
//        Message msg3 = new Message();
//        msg3.setTopic("topic3");
//        msg3.setBody("body3");
//
//        Message msg4 = new Message();
//        msg4.setTopic("topic4");
//        msg4.setBody("body4");

        FileStorage fileStorage = new FileStorage("topic1", 0);
        Position position1 = fileStorage.writeMsg2Disk(msg1);
        Position position2 = fileStorage.writeMsg2Disk(msg2);
//        Position position3 = fileStorage.writeMsg2Disk(msg3);
//        Position position4 = fileStorage.writeMsg2Disk(msg4);

        Message rs1 = fileStorage.loadMsg(position1);
        System.out.println(rs1);
        Message rs2 = fileStorage.loadMsg(position2);
        System.out.println(rs2);

        TreeMap index = fileStorage.loadIndex();
        System.out.println(index);

//
//        Message rs3 = fileStorage.loadMsg(position3);
//        System.out.println(rs3);
//
//        Message rs4 = fileStorage.loadMsg(position4);
//        System.out.println(rs4);

    }

}
