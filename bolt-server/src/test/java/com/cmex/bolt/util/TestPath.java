package com.cmex.bolt.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TestPath {

    @Test
    public void testJournalPath() throws IOException {
        FileChannel channel = FileChannel.open(Path.of("journal.data"), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        channel.write(ByteBuffer.wrap("Journal".getBytes()));
        System.out.println(Path.of("journal.data").toFile().exists());
    }
}
