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

@Slf4j
public class FileStorage implements Persistence {

    public FileStorage() {
        try {
            this.dbChannel = new RandomAccessFile(new File(FILE_NAME), "rwd").getChannel();
            this.positionChannel = new RandomAccessFile(new File(FILE_OFFSET), "rwd").getChannel();
            this.nextOffset = getLatestOffset();
        } catch (FileNotFoundException e) {
            log.error("FileStorage Init DB File error", e);
            throw new StorageException(e);
        }
        ;
    }

    private final String FILE_NAME = "db.data";
    private final String FILE_OFFSET = "db.offset";

    private FileChannel dbChannel;
    private FileChannel positionChannel;

    private long nextOffset;

    private long getLatestOffset() {
        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        offsetBuffer.limit();
        try {
            positionChannel.read(offsetBuffer, 0);
            return offsetBuffer.getLong(0);
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
            byte[] bytes = mapper.writeValueAsBytes(msg);
            int length = bytes.length;

            long offset = nextOffset;
            nextOffset += length;

            // Write NextOffset In DB
            ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
            offsetBuffer.putLong(nextOffset);
            int rs = this.positionChannel.write(ByteBuffer.wrap(offsetBuffer.array()), 0);

            this.dbChannel.write(ByteBuffer.wrap((bytes)), offset);
            Position position = new Position();
            position.setOffset(offset);
            position.setLength(length);
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

    public static void main(String[] args) {
        Message msg1 = new Message();
        msg1.setTopic("topic1");
        msg1.setBody("body1");

        Message msg2 = new Message();
        msg2.setTopic("topic2");
        msg2.setBody("body2");

        Message msg3 = new Message();
        msg3.setTopic("topic3");
        msg3.setBody("body3");

        Message msg4 = new Message();
        msg4.setTopic("topic4");
        msg4.setBody("body4");

        FileStorage fileStorage = new FileStorage();
        Position position1 = fileStorage.writeMsg2Disk(msg1);
        Position position2 = fileStorage.writeMsg2Disk(msg2);
        Position position3 = fileStorage.writeMsg2Disk(msg3);
        Position position4 = fileStorage.writeMsg2Disk(msg4);

        Message rs1 = fileStorage.loadMsg(position1);
        System.out.println(rs1);

        Message rs2 = fileStorage.loadMsg(position2);
        System.out.println(rs2);

        Message rs3 = fileStorage.loadMsg(position3);
        System.out.println(rs3);

        Message rs4 = fileStorage.loadMsg(position4);
        System.out.println(rs4);

    }

}
