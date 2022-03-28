package se.irori.kafka.claimcheck;

import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Utility methods for Claim Check related Kafka headers.
 */
public class ClaimCheckUtils {
  /**
   * Check if record headers indicate the message is a Claim Check.
   */
  public static boolean isClaimCheck(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::key)
        .anyMatch(ClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK::equals);
  }

  /**
   * Check if record headers indicate the message has a Claim Check processing error.
   */
  public static boolean isClaimCheckError(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::key)
        .anyMatch(ClaimCheckProducerInterceptor
            .HEADER_MESSAGE_CLAIM_CHECK_ERROR::equals);
  }

  /**
   * Get claim check reference from header.
   *
   * @param headers the Kafka message headers to process
   */
  public static byte[] getClaimCheckRefFromHeader(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .filter(h ->
            ClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK.equals(h.key()))
        .findFirst()
        .map(Header::value)
        .orElse(null);
  }

  /**
   * Get the stacktrace String if headers contain
   * a Claim Check processing error header.
   *
   * @param headers message headers
   * @return the error stacktrace, or null if not found
   */
  public static String getClaimCheckErrorStackTraceFromHeader(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .filter(h -> ClaimCheckProducerInterceptor
            .HEADER_MESSAGE_CLAIM_CHECK_ERROR.equals(h.key()))
        .findFirst()
        .map(h -> new String(h.value(), StandardCharsets.UTF_8))
        .orElse(null);
  }
}
