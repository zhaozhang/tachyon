package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.thrift.ClientStorePartitionInfo;
import tachyon.util.CommonUtils;

/**
 * Key/value store local
 */
public class KVWorkerStorage {
  private final String MASTER_ADDRESS;
  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;
  private HashMap<Integer, TachyonByteBuffer> mData;

  KVWorkerStorage(String masterAddress) throws IOException {
    MASTER_ADDRESS = masterAddress;
    mTFS = TachyonFS.get(MASTER_ADDRESS);
    mData = new HashMap<Integer, TachyonByteBuffer>();
  }

  public ByteBuffer getValue(ClientStorePartitionInfo info, ByteBuffer key) throws IOException {
    validate(info.getDataFileId());
    validate(info.getIndexFileId());

    TachyonByteBuffer dataBuffer = mData.get(info.getDataFileId());
    TachyonByteBuffer indexBuffer = mData.get(info.getIndexFileId());

    ByteBuffer result = null;

    ByteBuffer data = dataBuffer.DATA;
    // HashMap<byte[], byte[]> kv = new HashMap<byte[], byte[]>();
    while (data.hasRemaining()) {
      int size = data.getInt();
      byte[] tKey = new byte[size];
      data.get(tKey);
      size = data.getInt();
      byte[] tValue = new byte[size];
      data.get(tValue);
      if (CommonUtils.compare(tKey, key.array()) == 0) {
        result = ByteBuffer.allocate(tValue.length);
        result.put(tValue);
        result.flip();
        break;
      }
    }
    dataBuffer.DATA.clear();

    return result;
  }

  private synchronized void validate(int fileId) throws IOException {
    if (mData.containsKey(fileId)) {
      return;
    }
    TachyonFile file = mTFS.getFile(fileId);
    mData.put(fileId, file.readByteBuffer());
  }
}
