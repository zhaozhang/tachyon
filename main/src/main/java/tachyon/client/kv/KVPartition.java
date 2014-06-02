package tachyon.client.kv;

import java.nio.ByteBuffer;

import tachyon.client.TachyonFile;

/**
 * Each partition contains key/value pairs, and indices.
 * 
 * This class should be abstract, and have differen kinds of implementations. But for the first
 * step, it has only one implemention.
 */
public class KVPartition {
  private final KVStore KV_STORE;
  private final int INDEX;
  private final boolean CREATE;

  // private final Serializer KEY_SER;
  // private final Serializer VALUE_SER;

  private TachyonFile mKeyFile;
  private TachyonFile mValueFile;
  private TachyonFile mIndexFile;

  public static KVPartition createKVPartition(KVStore kvStore, int index) {
    return null;
  }

  public static KVPartition getKVPartition(KVStore kvStore, int index) {
    return null;
  }

  KVPartition(KVStore kvStore, int index, boolean create) {
    KV_STORE = kvStore;
    INDEX = index;
    CREATE = create;

    if (create) {

    } else {

    }
  }

  public void put(ByteBuffer key, ByteBuffer value) {
  }

  public void put(String key, int value) {
  }

  public ByteBuffer get(ByteBuffer key) {
    return null;
  }

  public void close() {

  }
}
