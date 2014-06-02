package tachyon.kv;

import java.nio.ByteBuffer;

/**
 * This is one type of partition.
 */
public class KVPartitionInfo {
  public final int KVINODE_ID;
  public final int PARTITION_INODE_ID;
  public final int INDEX;

  public final int KEY_INODE_ID;
  public final int VALUE_INODE_ID;
  public final int INDEX_INODE_ID;

  public final ByteBuffer START;
  public final ByteBuffer END;

  KVPartitionInfo(int kvInodeId, int partitionInodeId, int index, ByteBuffer start,
      ByteBuffer end, int keyInodeId, int valueInodeId, int indexInodeId) {
    KVINODE_ID = kvInodeId;
    PARTITION_INODE_ID = partitionInodeId;
    INDEX = index;

    KEY_INODE_ID = keyInodeId;
    VALUE_INODE_ID = valueInodeId;
    INDEX_INODE_ID = indexInodeId;

    // TODO make this cleaner and safer.
    START = start;
    END = end;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("KVPartitionInfo");
    sb.append("(KVINODE_ID ").append(KVINODE_ID);
    sb.append(", PARTITION_INODE_ID ").append(PARTITION_INODE_ID);
    sb.append(", INDEX ").append(INDEX);
    sb.append(", KEY_INODE_ID ").append(KEY_INODE_ID);
    sb.append(", VALUE_INODE_ID ").append(VALUE_INODE_ID);
    sb.append(", INDEX_INODE_ID ").append(INDEX_INODE_ID);
    sb.append(", Start ").append(START.toString());
    sb.append(", End ").append(END.toString());
    sb.append(")");
    return sb.toString();
  }
}
