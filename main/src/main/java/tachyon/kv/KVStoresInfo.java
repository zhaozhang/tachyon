package tachyon.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import tachyon.thrift.ClientStorePartitionInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.TachyonException;

/**
 * All key/value stores information in the master;
 */
public class KVStoresInfo {
  public Map<Integer, KVStoreInfo> KVS = new HashMap<Integer, KVStoreInfo>();

  public synchronized void addKVStoreInfo(KVStoreInfo info) throws FileAlreadyExistException {
    if (KVS.containsKey(info.INODE_ID)) {
      throw new FileAlreadyExistException("The store already exists: " + info);
    }

    KVS.put(info.INODE_ID, info);
  }

  public synchronized boolean addPartition(ClientStorePartitionInfo info) throws TachyonException,
      IOException {
    int storeId = info.storeId;
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist for partition: " + info);
    }
    KVS.get(storeId).addPartition(
        new KVPartitionInfo(info.storeId, info.partitionIndex, info.dataFileId, info.indexFileId,
            info.startKey, info.endKey));
    return true;
  }

  public synchronized KVPartitionInfo get(int storeId, ByteBuffer key) throws TachyonException {
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return KVS.get(storeId).getPartition(key);
  }

  public synchronized KVPartitionInfo get(int storeId, int partitionIndex) throws TachyonException {
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return KVS.get(storeId).getPartition(partitionIndex);
  }
}
