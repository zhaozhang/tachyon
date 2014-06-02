package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

import org.apache.hadoop.util.hash.Hash;

import tachyon.Pair;
import tachyon.client.TachyonFS;

/**
 * Key/value store local
 */
public class KVWorkerStorage {
  // TODO Using TachyonFS is a trick for now.

  private TachyonFS mTFS;
  private HashMap<Pair<Integer, Integer>> mInChargeKVPartitions;

  private Hash

  KVWorkerStorage(String masterAddress) throws IOException {
    mTFS = TachyonFS.get(masterAddress);
    mInChargeKVPartitions = new HashSet<Pair<Integer, Integer>>();
  }

  ByteBuffer get(int kvInodeId, int partitionId, ByteBuffer buf) {
    if (!mInChargeKVPartitions.contains(new Pair<Integer, Integer>(kvInodeId, partitionId))) {
      // TODO this assume this will not fail.
      queryMaster(kvInodeId, partitionId);
    }

  }
}
