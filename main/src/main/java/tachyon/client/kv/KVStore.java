package tachyon.client.kv;

import java.nio.ByteBuffer;

/**
 * The store is identified by its path, e.g., /data/dailyreport/2014-12-05
 * 
 * This is the client a user uses. It should have put, get, and delete functions. In the first step,
 * these methods are only implemented in the KVPartition class.
 * 
 * This class should be abstract. But for the first step, it only has one implementation.
 */
public class KVStore {
  private final String KV_STORE_PATH;

  public static KVStore get(String kvStorePath) {
    return null;
  }

  KVStore(String kvStorePath) {
    KV_STORE_PATH = kvStorePath;
  }

  public KVPartition createPartition(int index) {
    return KVPartition.createKVPartition(this, index);
  }

  public ByteBuffer get(ByteBuffer key) {
    return null;
  }
}
