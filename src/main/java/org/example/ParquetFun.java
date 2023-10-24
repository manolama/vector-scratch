package org.example;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.io.SeekableInputStream;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;

public class ParquetFun {
  public static final String MAGIC_STR = "PAR1";
  public static final byte[] MAGIC = MAGIC_STR.getBytes(StandardCharsets.US_ASCII);

  ByteBuffer buf = ByteBuffer.allocate(4096);
  final LocalInputFile lif;
  final SeekableInputStream stream;
  public ParquetFun(String file) throws Exception {
    var fileLen = new File(file).length();
    int FOOTER_LENGTH_SIZE = 4;

    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(file + " is not a Parquet file (length is too low: " + fileLen + ")");
    }
    lif = new LocalInputFile(Path.of(file));
    stream = lif.newStream();
    long fileMetadataLengthIndex = fileLen - MAGIC.length - FOOTER_LENGTH_SIZE;
    stream.seek(fileMetadataLengthIndex);
    int metaSize = readIntLittleEndian(stream);// read footer length
    System.out.println("Meta len: " + metaSize);
    byte[] magic = new byte[MAGIC.length];
    stream.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException("Whoops, not a PArquet file");
    }

    long fileMetadataIndex = fileMetadataLengthIndex - metaSize;
    System.out.println("read footer length: " + metaSize + ", footer index: " + fileMetadataIndex);
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    stream.seek(fileMetadataIndex);

    if (buf.capacity() < metaSize) {
      buf = ByteBuffer.allocate(metaSize);
    }
    stream.readFully(buf);

    System.out.println("Finished to read all footer bytes.");
    buf.flip();
    InputStream footerBytesStream = ByteBufferInputStream.wrap(buf);

    var metaData = new FileMetaData();
    var tiost = new TIOStreamTransport(footerBytesStream);
    var tcp = new TCompactProtocol(tiost);
    metaData.read(tcp);
    System.out.println("--------- META ------------");
    System.out.println(metaData);

    System.out.println("---------- Blocks -------------");
    System.out.println(generateRowGroupOffsets(metaData));
  }

  // from PArquetMetadataConverter.java
  private Map<RowGroup, Long> generateRowGroupOffsets(FileMetaData metaData) {
    Map<RowGroup, Long> rowGroupOrdinalToRowIdx = new HashMap<>();
    List<RowGroup> rowGroups = metaData.getRow_groups();
    if (rowGroups != null) {
      long rowIdxSum = 0;
      for (int i = 0; i < rowGroups.size(); i++) {
        rowGroupOrdinalToRowIdx.put(rowGroups.get(i), rowIdxSum);
        rowIdxSum += rowGroups.get(i).getNum_rows();
      }
    }
    return rowGroupOrdinalToRowIdx;
  }

  public void close() {
    try {
      stream.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    var pf = new ParquetFun("/Users/clarsen/Documents/netflix/temp/trace_playground/tempo_00.parquet");
    pf.close();
  }
}
