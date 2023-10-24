package org.example;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * {@code LocalInputFile} is an implementation needed by Parquet to read
 * from local data files using {@link SeekableInputStream} instances.
 */
public class LocalInputFile implements InputFile {

  private final Path path;
  private long length = -1;

  public LocalInputFile(Path file) {
    path = file;
  }

  @Override
  public long getLength() throws IOException {
    if (length == -1) {
      try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
        length = file.length();
      }
    }
    return length;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {

    return new SeekableInputStream() {

      private final RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r");

      @Override
      public int read() throws IOException {
        return randomAccessFile.read();
      }

      @Override
      public long getPos() throws IOException {
        return randomAccessFile.getFilePointer();
      }

      @Override
      public void seek(long newPos) throws IOException {
        randomAccessFile.seek(newPos);
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        randomAccessFile.readFully(bytes);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        randomAccessFile.readFully(bytes, start, len);
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        int code = read(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
        return code;
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        readFully(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
      }

      @Override
      public void close() throws IOException {
        randomAccessFile.close();
      }
    };
  }
}
