package tachyon.client.kv;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.util.CommonUtils;

/**
 * Each partition contains key/value pairs, and indices.
 * 
 * This class should be abstract, and have differen kinds of implementations. But for the first
 * step, it has only one implemention.
 */
public class KVPartition {
  public static KVPartition createKVPartition(KVStore kvStore, int index) throws IOException {
    return new KVPartition(kvStore, index, true);
  }

  public static KVPartition getKVPartition(KVStore kvStore, int index) throws IOException {
    return new KVPartition(kvStore, index, false);
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final KVStore KV_STORE;
  private final int INDEX;
  private final boolean CREATE;
  private final TachyonFS TFS;

  // private final Serializer KEY_SER;
  // private final Serializer VALUE_SER;

  private String mStorePath;
  private String mPartitionPath;

  private String mDataFilePath;
  private TachyonFile mDataFile;
  private int mDataFileId;
  private OutStream mDataFileOutStream;

  private String mIndexFilePath;
  private TachyonFile mIndexFile;
  private int mIndexFileId;
  private OutStream mIndexFileOutStream;

  private ByteBuffer mStartKey;
  private ByteBuffer mEndKey;
  private int mDataFileLocation;

  KVPartition(KVStore kvStore, int index, boolean create) throws IOException {
    KV_STORE = kvStore;
    INDEX = index;
    CREATE = create;
    TFS = KV_STORE.getTFS();

    String tPath = KV_STORE.getStorePath();
    mPartitionPath = tPath.substring(tPath.indexOf("//") + 2);
    mStorePath = mPartitionPath.substring(mPartitionPath.indexOf("/"));
    mPartitionPath =
        CommonUtils.concat(mStorePath, "partition-" + CommonUtils.addLeadingZero(index, 5));
    mDataFilePath = mPartitionPath + "-data";
    mIndexFilePath = mPartitionPath + "-index";
    LOG.info("Creating KV partition: " + toString());

    if (create) {
      mDataFileId = TFS.createFile(mDataFilePath, Constants.GB);
      mDataFile = TFS.getFile(mDataFileId);
      mDataFileOutStream = mDataFile.getOutStream(WriteType.CACHE_THROUGH);

      mIndexFileId = TFS.createFile(mIndexFilePath, Constants.GB);
      mIndexFile = TFS.getFile(mIndexFileId);
      mIndexFileOutStream = mIndexFile.getOutStream(WriteType.CACHE_THROUGH);

      if (mDataFileId == -1 || mIndexFileId == -1) {
        throw new IOException("Failed to create data file or index file, or both.");
      }
    } else {
      mDataFile = TFS.getFile(mDataFilePath);
      mIndexFile = TFS.getFile(mIndexFilePath);
    }

    mDataFileLocation = 0;
    mStartKey = null;
    mEndKey = null;

  }

  public void close() throws IOException {
    if (CREATE) {
      mDataFileOutStream.close();
      mIndexFileOutStream.close();
      // TFS.addKVPartition(mStorePath, INDEX, mDataFileId, mIndexFileId, mStartKey, mEndKey);
    }
  }

  public ByteBuffer get(ByteBuffer key) {
    return null;
  }

  public void put(byte[] key, byte[] value) throws IOException {
    if (!CREATE) {
      throw new IOException("Can not put key value pair in non-create mode");
    }
    // put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    if (mStartKey == null) {
      mStartKey = ByteBuffer.allocate(key.length);
      mStartKey.put(key);
      mStartKey.flip();
    }
    mStartKey = ByteBuffer.allocate(value.length);
    mStartKey.put(value);
    mStartKey.flip();
    // mEndKey = ByteBuffer.wrap(value);

    mIndexFileOutStream.write(ByteBuffer.allocate(4).putInt(mDataFileLocation).array());
    mDataFileOutStream.write(ByteBuffer.allocate(4).putInt(key.length).array());
    mDataFileOutStream.write(key);
    mDataFileOutStream.write(ByteBuffer.allocate(4).putInt(value.length).array());
    mDataFileOutStream.write(value);
    mDataFileLocation += 4 + key.length + 4 + value.length;
  }

  public void put(String key, int value) throws IOException {
    put(key.getBytes(), ByteBuffer.allocate(4).putInt(value).array());
  }

  // public void put(ByteBuffer key, ByteBuffer value) {
  // }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("KVPartition(");
    sb.append("CREATE ").append(CREATE);
    sb.append(" , mStorePath ").append(mStorePath);
    sb.append(" , mPartitionPath ").append(mPartitionPath);
    sb.append(" , mDataFilePath ").append(mDataFilePath);
    sb.append(" , mIndexFilePath ").append(mIndexFilePath);
    sb.append(")");
    return sb.toString();
  }
}
