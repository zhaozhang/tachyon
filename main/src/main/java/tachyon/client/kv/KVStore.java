package tachyon.client.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
import tachyon.thrift.ClientStorePartitionInfo;
import tachyon.util.CommonUtils;

/**
 * The store is identified by its path, e.g., /data/dailyreport/2014-12-05
 * 
 * This is the client a user uses. It should have put, get, and delete functions. In the first step,
 * these methods are only implemented in the KVPartition class.
 * 
 * This class should be abstract. But for the first step, it only has one implementation.
 */
public class KVStore {
  public static KVStore create(String kvStorePath) throws IOException {
    return new KVStore(kvStorePath, true);
  }

  public static KVStore get(String kvStorePath) throws IOException {
    return new KVStore(kvStorePath, false);
  }

  private final String KV_STORE_PATH;
  private final String KV_STORE_PATH_NOSCHEMA;
  private final int STORE_ID;
  private TachyonFS TFS;

  private List<ClientStorePartitionInfo> mPartitions = new ArrayList<ClientStorePartitionInfo>();

  KVStore(String kvStorePath, boolean create) throws IOException {
    KV_STORE_PATH = kvStorePath;
    KV_STORE_PATH_NOSCHEMA = CommonUtils.getPathWithoutSchema(KV_STORE_PATH);
    TFS = TachyonFS.get(KV_STORE_PATH);

    if (create) {
      STORE_ID = TFS.kv_createStore(KV_STORE_PATH_NOSCHEMA);
    } else {
      STORE_ID = TFS.getFileId(KV_STORE_PATH_NOSCHEMA);
    }
  }

  public KVPartition createPartition(int index) throws IOException {
    return KVPartition.createKVPartition(TFS, STORE_ID, KV_STORE_PATH_NOSCHEMA, index);
  }

  public ByteBuffer get(byte[] key) throws IOException {
    ByteBuffer tKey = ByteBuffer.wrap(key);
    ClientStorePartitionInfo partition = getKVPartition(tKey);
    if (partition == null) {
      partition = TFS.kv_getPartitionWithStoreId(STORE_ID, tKey);
      if (partition == null) {
        return null;
      }
      for (int k = mPartitions.size(); k < partition.partitionIndex; k ++) {
        mPartitions.add(null);
      }
      mPartitions.set(partition.partitionIndex, partition);
    }

    return TFS.kv_getValue(partition, tKey);
  }

  private ClientStorePartitionInfo getKVPartition(ByteBuffer key) {
    for (int k = 0; k < mPartitions.size(); k ++) {
      ClientStorePartitionInfo partition = mPartitions.get(k);
      if (partition == null) {
        continue;
      }
      if (partition.startKey.compareTo(key) <= 0 && partition.endKey.compareTo(key) >= 0) {
        return partition;
      }
    }

    return null;
  }

  public int getStoreId() {
    return STORE_ID;
  }

  public String getStorePath() {
    return KV_STORE_PATH;
  }

  TachyonFS getTFS() {
    return TFS;
  }
}
