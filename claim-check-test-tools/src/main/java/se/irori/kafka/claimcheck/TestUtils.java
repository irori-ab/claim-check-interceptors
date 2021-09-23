package se.irori.kafka.claimcheck;


import java.util.Base64;
import java.util.Random;

/**
 * Common utility methods for tests.
 */
public final class TestUtils {

  /**
   * Random seed.
   */
  private static final Random RANDOM = new Random(0L);

  private TestUtils() {
  }

  /**
   * Generates random strings of a given length.
   *
   * @param length length of string.
   *
   * @return the random string.
   */
  public static String getRandomString(final int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    Base64.Encoder encoder = Base64.getEncoder();

    return encoder.encodeToString(bytes).substring(0, length);
  }

  /**
   * Generates random byte arrays of a given length.
   *
   * @param length length of array.
   *
   * @return the random byte array.
   */
  public static byte[] getRandomBytes(final int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }
}
