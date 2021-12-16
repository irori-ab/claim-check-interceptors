package se.irori.kafka.claimcheck;

import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class ClaimCheckUtils {
  /**
   * Check if record is a Claim Check.
   */
  public static boolean isClaimCheck(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::key)
        .anyMatch(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK::equals);
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
}
