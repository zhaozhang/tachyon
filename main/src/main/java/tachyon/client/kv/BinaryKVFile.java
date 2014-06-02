package tachyon.client.kv;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;

public class BinaryKVFile {
  private final TachyonFS TFS;
  private final String FILE_PATH;

  private TachyonFile mDataFile;
  private TachyonFile mIndexFile;
  private boolean mOpen;
  private boolean mRead;

  public static synchronized BinaryKVFile get(TachyonFS tfs, String filePath) throws IOException {
    return new BinaryKVFile(tfs, filePath);
  }

  BinaryKVFile(TachyonFS tfs, String filePath) {
    TFS = tfs;
    FILE_PATH = filePath;
    mOpen = false;
  }

  public void open(boolean read) {
  }

  public void close() {
  }

  public void put(ByteBuffer key, ByteBuffer value) {
  }

  public ByteBuffer get(ByteBuffer key) {
    return null;
  }
}
