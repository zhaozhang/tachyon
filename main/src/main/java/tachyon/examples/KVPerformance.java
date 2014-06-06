package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.Version;
import tachyon.client.kv.KVStore;
import tachyon.thrift.TachyonException;

public class KVPerformance {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static String KV_STORE_ADDRESS = null;
  private static KVStore KVS = null;
  private static List<String> KEYS = new ArrayList<String>();

  public static void main(String[] args) throws IOException, TachyonException, TException {
    if (args.length != 1) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar tachyon.examples.KVPerformance <KVStoreAddress>");
      System.exit(-1);
    }

    KV_STORE_ADDRESS = args[0];
    KVS = KVStore.get(KV_STORE_ADDRESS);

    KEYS.add("the");
    KEYS.add("Apache");
    KEYS.add("any");
    KEYS.add("Spark");
    KEYS.add("spark");

    int tests = 100000;
    int total = 0;
    long startTimeMs = System.currentTimeMillis();
    for (int k = 0; k < tests; k ++) {
      String key = KEYS.get(k % 5);
      ByteBuffer result = KVS.get(key.getBytes());
      if (result.limit() == 0) {
        // LOG.info("Key " + key + " does not exist in the store.");
      } else {
        int res = result.getInt();
        total += res;
        // LOG.info("(" + key + "," + res + ")");
      }
    }
    long endTimeMs = System.currentTimeMillis();

    LOG.info("Total value is " + total);
    LOG.info("Total time MS: " + (endTimeMs - startTimeMs));
    LOG.info("Average time: " + (endTimeMs - startTimeMs) * 1.0 / tests);
    System.exit(0);
  }
}
