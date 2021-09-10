package se.irori.kafka.claimcheck;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Serializer;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig.Keys;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;

/**
 * Implementation of the ClaimCheck pattern producer side.
 */
public class SerializingClaimCheckProducerInterceptor<K, V>
    implements ProducerInterceptor<K, V> {

  public static final String HEADER_MESSAGE_CLAIM_CHECK = "message-claim-check";

  private long checkinUncompressedSizeOverBytes =
      BaseClaimCheckConfig.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT;

  private Serializer<K> keySerializer;

  private Serializer<V> valueSerializer;

  private ClaimCheckBackend claimCheckBackend;

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    checkinUncompressedSizeOverBytes = baseClaimCheckConfig.getLong(
      Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG);

    this.valueSerializer = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);

    this.keySerializer = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);

    this.claimCheckBackend = baseClaimCheckConfig.getConfiguredInstance(
        Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ClaimCheckBackend.class);
  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    final byte[] keyBytes = keySerializer.serialize(
          producerRecord.topic(),
          producerRecord.headers(),
          producerRecord.key()
    );

    final byte[] valueBytes = valueSerializer.serialize(
          producerRecord.topic(),
          producerRecord.headers(),
          producerRecord.value()
    );

    if (isAboveClaimCheckLimit(producerRecord, keyBytes, valueBytes)) {
      ClaimCheck claimCheck = claimCheckBackend.checkIn(new ProducerRecord<>(producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          null,
          valueBytes,
          null)
      );

      return new ProducerRecord<>(producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          producerRecord.key(),
          null, // TODO: Fix how it interacts with log compaction
          producerRecord.headers().add(HEADER_MESSAGE_CLAIM_CHECK, claimCheck.serialize())
      );
    } else {
      return producerRecord;
    }
  }

  private boolean isAboveClaimCheckLimit(ProducerRecord<?, ?> originalRecord,
                                        byte[] keyBytes, byte[] valueBytes) {
    Headers headers = originalRecord.headers();
    long timestamp = originalRecord.timestamp() == null ? 0 : originalRecord.timestamp();
    int batchSizeInBytes = DefaultRecordBatch.sizeInBytes(Collections.singleton(
        new SimpleRecord(timestamp, keyBytes, valueBytes,
            headers.toArray())));

    return batchSizeInBytes > checkinUncompressedSizeOverBytes;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    // do nothing by default
  }

  @Override
  public void close() {
    keySerializer.close();
    valueSerializer.close();
    claimCheckBackend.close();
  }
}
