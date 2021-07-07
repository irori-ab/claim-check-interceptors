package se.irori.kafka.claimcheck;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Abstract implementation of the ClaimCheck pattern producer side.
 */
public abstract class AbstractClaimCheckProducerInterceptor
    implements ProducerInterceptor<byte[], byte[]> {
  public static final String CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG
      = "claimcheck.checkin.uncompressed-size.over.bytes";

  public static final String HEADER_MESSAGE_IS_CLAIM_CHECK
      = "message-is-claim-check";

  // TODO: does this account for headers as well?
  private int checkinUncompressedSizeOverBytes = 1048588;

  public abstract ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord);

  @Override
  public void configure(Map<String, ?> configs) {
    Object maxSizeConfig = configs.get(CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG);
    if (maxSizeConfig != null) {
      if (maxSizeConfig instanceof Integer) {
        checkinUncompressedSizeOverBytes = (Integer) maxSizeConfig;
      } else {
        try {
          checkinUncompressedSizeOverBytes = Integer.parseInt(maxSizeConfig.toString());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG
              + " is not an accepted number", e);
        }
      }
    }
  }

  @Override
  public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> producerRecord) {
    long headerTotalSize = 0;

    for (Header header : producerRecord.headers()) {
      headerTotalSize += header.key().length() + header.value().length;
    }

    long keySize = producerRecord.key() == null ? 0 : producerRecord.key().length;
    long valueSize = producerRecord.value() == null ? 0 : producerRecord.value().length;

    if (keySize + valueSize + headerTotalSize > checkinUncompressedSizeOverBytes) {
      ClaimCheck claimCheck = checkIn(producerRecord);
      return new ProducerRecord<>(producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          producerRecord.key(),
          claimCheck.serialize(),
          producerRecord.headers().add(HEADER_MESSAGE_IS_CLAIM_CHECK, null) // marker header
      );
    } else {
      return producerRecord;
    }
  }
}
