package se.irori.kafka.claimcheck;


import java.util.Base64;
import java.util.Random;

/**
 * Common utility methods for tests.
 */
public class TestUtils {

  private static Random RANDOM = new Random(0L);
  /**
   * @return a random string of the given length
   */
  public static String getRandomString(int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    Base64.Encoder encoder = Base64.getEncoder();

    return encoder.encodeToString(bytes).substring(0, length);
  }

  public static byte[] getRandomBytes(int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }
}
