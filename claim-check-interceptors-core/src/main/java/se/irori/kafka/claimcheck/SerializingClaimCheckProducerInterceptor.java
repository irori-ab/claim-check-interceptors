package se.irori.kafka.claimcheck;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig.Keys;

/**
 * Implementation of the ClaimCheck pattern producer side.
 */
public class SerializingClaimCheckProducerInterceptor<K, V>
    implements ProducerInterceptor<K, V> {

  public static final Logger LOG =
      LoggerFactory.getLogger(SerializingClaimCheckProducerInterceptor.class);

  public static final String HEADER_MESSAGE_CLAIM_CHECK = "message-claim-check";
  public static final String HEADER_MESSAGE_CLAIM_CHECK_ERROR = "message-claim-check-error";

  private long checkinUncompressedSizeOverBytes =
      BaseClaimCheckConfig.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT;

  private Serializer<K> keySerializer;

  private Serializer<V> valueSerializer;

  private ClaimCheckBackend claimCheckBackend;

  private boolean isWrappingSerializerConfigured;

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    checkinUncompressedSizeOverBytes = baseClaimCheckConfig.getLong(
      Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG);

    this.valueSerializer = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
    this.valueSerializer.configure(baseClaimCheckConfig.originals(), false);

    this.keySerializer = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    this.keySerializer.configure(baseClaimCheckConfig.originals(), true);

    this.claimCheckBackend = baseClaimCheckConfig.getConfiguredInstance(
        Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ClaimCheckBackend.class);

    // TODO: WARN if ClaimCheckWrappingSerializer is not configured
  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    try {
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
      int valueBytesLength = valueBytes == null ? 0 : valueBytes.length;


      if (isAboveClaimCheckLimit(producerRecord, keyBytes, valueBytes)) {
        ClaimCheck claimCheck = claimCheckBackend.checkIn(new ProducerRecord<>(producerRecord.topic(),
            producerRecord.partition(),
            producerRecord.timestamp(),
            null,
            valueBytes,
            null)
        );

        LOG.debug("checked in claim check: topic={}, key={}, ref={}, length={}",
            producerRecord.topic(), producerRecord.key(), claimCheck.getReference(),
            valueBytesLength);

        // note: if using ClaimCheckWrappingSerializer this can probably be made to work
        // somewhat with log compaction, since null will be replaced
        return new ProducerRecord<>(producerRecord.topic(),
            producerRecord.partition(),
            producerRecord.timestamp(),
            producerRecord.key(),
            null,
            producerRecord.headers().add(HEADER_MESSAGE_CLAIM_CHECK, claimCheck.serialize())
        );
      } else {
        LOG.debug("not checking in claim check: topic={}, key={}, length={}",
            producerRecord.topic(), producerRecord.key(), valueBytesLength);
        return producerRecord;
      }
    } catch (Exception e) {
      LOG.error("Error when processing claim check", e);
      // exception that would have been silent for producer
      // propagate for ClaimCheckWrappingSerializer to pick up and rethrow
      StringWriter stackTraceWriter = new StringWriter();
      PrintWriter out = new PrintWriter(stackTraceWriter);
      e.printStackTrace(out);

      // if user forgot to configure ClaimCheckWrappingSerializer we just leave
      // payload as is, so that we don't silently lose it without an error propagated
      return new ProducerRecord<>(producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          producerRecord.key(),
          producerRecord.value(), // copy as is
          producerRecord.headers().add(HEADER_MESSAGE_CLAIM_CHECK_ERROR,
              stackTraceWriter.toString().getBytes(StandardCharsets.UTF_8))
      );
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
