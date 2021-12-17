package se.irori.kafka.claimcheck;

import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class ClaimCheckUtils {
  /**
   * Check if record headers indicate the message is a Claim Check.
   */
  public static boolean isClaimCheck(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::key)
        .anyMatch(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK::equals);
  }

  /**
   * Check if record headers indicate the message has a Claim Check processing error.
   */
  public static boolean isClaimCheckError(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::key)
        .anyMatch(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_ERROR::equals);
  }

  /**
   * Get claim check reference from header.
   *
   * @param headers the Kafka message headers to process
   */
  public static byte[] getClaimCheckRefFromHeader(Headers headers) {
    byte[] ret = null;
    for (Header header : headers) {
      if (SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK
          .equals(header.key())) {
        ret = header.value();
      }
    }
    return ret;
  }

  /**
   * Get the stacktrace String if headers contain
   * a Claim Check processing error header.
   *
   * @param headers message headers
   * @return the error stacktrace, or null if not found
   */
  public static String getClaimCheckErrorStackTraceFromHeader(Headers headers) {
    String stackTrace = null;
    for (Header header : headers) {
      if (SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_ERROR
          .equals(header.key())) {
        stackTrace = new String(header.value(), StandardCharsets.UTF_8);
      }
    }
    return stackTrace;
  }
}
