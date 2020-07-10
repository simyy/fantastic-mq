package com.github.fantasticlab.mq.core.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.TreeMap;

@Slf4j
public class FileStorage implements Persistence {

    private String path;
    private String topic;
    private int queue;

    private String DB_DATA = "db.data";
    private String DB_INDEX = "db.index";
    private String DB_OFFSET = "db.offset";

    private FileChannel dbChannel;
    private FileChannel indexChannel;
    private FileChannel offsetChannel;
    private long nextOffsetOfData = 0;
    private long nextOffsetOfIndex = 0;
    private TreeMap<Long, Position> index = null;

    public FileStorage(String topic, int queue) {
        this(topic, queue, "");
    }

    public FileStorage(String topic, int queue, String path) {
        this.path = path;
        this.topic = topic;
        this.queue = queue;
        try {
            this.dbChannel = new RandomAccessFile(new File(path + topic + "-" + queue + "-" + DB_DATA), "rwd").getChannel();
            this.indexChannel = new RandomAccessFile(new File(path + topic + "-" + queue + "-" + DB_INDEX), "rwd").getChannel();
            this.offsetChannel = new RandomAccessFile(new File(path + topic + "-" + queue  + "-" + DB_OFFSET), "rwd").getChannel();
            loadFromDisk();
        } catch (FileNotFoundException e) {
            log.error("FileStorage Init DB File error", e);
            throw new StorageException(e);
        }
    }


    private void loadFromDisk() {
        loadOffset();
        loadIndex();
    }

    private void loadOffset() {
        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES * 2);
        offsetBuffer.order(ByteOrder.BIG_ENDIAN);
        try {
            offsetChannel.read(offsetBuffer);
            nextOffsetOfData = offsetBuffer.getLong(0);
            nextOffsetOfIndex = offsetBuffer.getLong(Long.BYTES);
        } catch (IOException e) {
            // ignore
        }
    }

    private void loadIndex() {
        this.index = new TreeMap();
        long start = 0;
        try {
            ByteBuffer indexBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            while (true) {
                int rs = indexChannel.read(indexBuffer, start);
                if (rs <= 0) {
                    break;
                }
                long offset = indexBuffer.getLong(0);
                long length = indexBuffer.getLong(Long.BYTES);
                index.put(offset, new Position(offset, length));
                start += Long.BYTES * 2;
                indexBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Position writeMsg(Message msg) {
        return null;
    }

    @Override
    synchronized public Position writeMsg2Disk(Message msg) {
        try {

            // Convert Msg to Bytes
            ObjectMapper mapper = new ObjectMapper();
            byte[] bytesOfData = mapper.writeValueAsBytes(msg);
            int lengthOfData = bytesOfData.length;

            // Reset Offset
            long offsetOfData = nextOffsetOfData;
            long offsetOfIndex = nextOffsetOfIndex;
            nextOffsetOfData += lengthOfData;
            nextOffsetOfIndex += Long.BYTES * 2;

            Position position = new Position(offsetOfData, lengthOfData);

            // Write Msg
            this.dbChannel.write(ByteBuffer.wrap((bytesOfData)), offsetOfData);

            // Write Offset
            ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            offsetBuffer.putLong(nextOffsetOfData);
            offsetBuffer.putLong(Long.BYTES, nextOffsetOfIndex);
            this.offsetChannel.write(ByteBuffer.wrap(offsetBuffer.array()), 0);

            // Write Index
            index.put(offsetOfData, position);
            ByteBuffer indexBuffer = ByteBuffer.allocate(Long.BYTES * 2);
            indexBuffer.putLong(offsetOfData);
            indexBuffer.putLong(Long.BYTES, lengthOfData);
            this.indexChannel.write(ByteBuffer.wrap(indexBuffer.array()), offsetOfIndex);

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

    @Override
    public Position getPosition(Long offset) {
        return index.get(offset);
    }

    @Override
    public Long getNextOffset(Long offset) {
        return offset + index.get(offset).getLength();
    }

    @Override
    public void close() {
        try {
            if (dbChannel != null) {
                dbChannel.close();
            }
            if (indexChannel != null) {
                indexChannel.close();
            }
            if (offsetChannel != null) {
                offsetChannel.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

}
