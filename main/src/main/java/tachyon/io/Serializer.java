package tachyon.io;

import java.nio.ByteBuffer;

/**
 * Data Serializer.
 * 
 * @param <T>
 */
public abstract class Serializer<T> {
  public abstract T fromByteBuffer(ByteBuffer byteBuffer);

  public abstract T fromBytes(byte[] bytes);

  public abstract byte[] toBytes();

  public abstract ByteBuffer toByteBuffer();
}
