package se.irori.kafka.claimcheck;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig.Keys;


/**
 * Implementation of the ClaimCheck pattern producer side. Assumes you have also configured
 * the {@link ClaimCheckSerializer} to catch any propagated errors.
 *
 * <p>If the message is above the configured limit, a claim check will be published
 * in the configured backend, and a reference stored as a header of the message
 * published in Kafka.
 */
public class ClaimCheckStreamingProducerInterceptor<K>
    implements ProducerInterceptor<K, InputStream> {

  private static LongSerializer payloadSizeSerializer = new LongSerializer();
  private static LongDeserializer payloadSizeDeserializer = new LongDeserializer();

  public static final Logger LOG =
      LoggerFactory.getLogger(ClaimCheckStreamingProducerInterceptor.class);

  public static final String HEADER_MESSAGE_CLAIM_CHECK = "message-claim-check";
  public static final String HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE =
      "message-claim-check-payload-size";
  public static final String HEADER_MESSAGE_CLAIM_CHECK_ERROR = "message-claim-check-error";

  private long checkinUncompressedSizeOverBytes =
      BaseClaimCheckConfig.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT;

  private Serializer<K> keySerializer;

  private ClaimCheckBackend claimCheckBackend;


  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    checkinUncompressedSizeOverBytes = baseClaimCheckConfig.getLong(
      Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG);


    this.keySerializer = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    this.keySerializer.configure(baseClaimCheckConfig.originals(), true);

    this.claimCheckBackend = baseClaimCheckConfig.getConfiguredInstance(
        Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ClaimCheckBackend.class);

    Serializer<?> rootSerializer = baseClaimCheckConfig
        .getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);

    if (!(rootSerializer instanceof ClaimCheckSerializer)) {
      throw new ConfigException("ClaimCheckProducerInterceptor must be used with"
          + " ClaimCheckSerializer as value.serializer to guarantee propagation of"
          + " exceptions to the client.");
    }
  }

  @Override
  public ProducerRecord<K, InputStream> onSend(ProducerRecord<K, InputStream> producerRecord) {
    try {
      long payloadSize = getPayloadSize(producerRecord.headers());

      final byte[] keyBytes = keySerializer.serialize(
          producerRecord.topic(),
          producerRecord.headers(),
          producerRecord.key()
      );

      InputStream payloadStream = producerRecord.value();

      if (isAboveClaimCheckLimit(producerRecord, keyBytes, payloadSize)) {
        LOG.debug("starting  claim check streaming upload: topic={}, key={}, length={}",
            producerRecord.topic(), producerRecord.key(), payloadSize);
        ClaimCheck claimCheck = claimCheckBackend.checkInStreaming(producerRecord.topic(),
            payloadStream,
            payloadSize);

        LOG.debug("checked in claim check streaming: topic={}, key={}, ref={}, length={}",
            producerRecord.topic(), producerRecord.key(), claimCheck.getReference(),
            payloadSize);

        // note: if using ClaimCheckSerializer this can probably be made to work
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
            producerRecord.topic(), producerRecord.key(), payloadSize);
        return producerRecord;
      }
    } catch (Exception e) {
      LOG.error("Error when processing claim check", e);
      // exception that would have been silent for producer
      // propagate for ClaimCheckSerializer to pick up and rethrow
      StringWriter stackTraceWriter = new StringWriter();
      PrintWriter out = new PrintWriter(stackTraceWriter);
      e.printStackTrace(out);

      return new ProducerRecord<>(producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          producerRecord.key(),
          // might as well pass the original to serializer for debugging purposes
          producerRecord.value(),
          producerRecord.headers().add(HEADER_MESSAGE_CLAIM_CHECK_ERROR,
              stackTraceWriter.toString().getBytes(StandardCharsets.UTF_8))
      );
    }
  }

  private boolean isAboveClaimCheckLimit(ProducerRecord<?, ?> originalRecord,
                                         byte[] keyBytes, long valueSize) {
    Headers headers = originalRecord.headers();
    long timestamp = originalRecord.timestamp() == null ? 0 : originalRecord.timestamp();
    int batchSizeInBytes = DefaultRecordBatch.sizeInBytes(Collections.singleton(
        new SimpleRecord(timestamp, keyBytes, new byte[] {},
            headers.toArray())));

    return batchSizeInBytes + valueSize > checkinUncompressedSizeOverBytes;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    // do nothing by default
  }

  @Override
  public void close() {
    keySerializer.close();
    claimCheckBackend.close();
  }


  /**
   * Get the payload size header from a set of message headers.
   *
   * @param headers the header set to process
   * @return the payload size, if header was found
   * @throws IllegalArgumentException if no header entry was found
   */
  public static long getPayloadSize(Headers headers) {
    Header payloadSizeHeader = headers.lastHeader(HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE);
    if (payloadSizeHeader == null) {
      throw new IllegalArgumentException("You must supply the '"
          + HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE + "' header in streaming mode.");
    }
    return payloadSizeDeserializer.deserialize("dummy", payloadSizeHeader.value());
  }

  /**
   * Set the payload size header on a set of headers.
   *
   * @param headers where to set the header
   * @param payloadSize value to set
   */
  public static void setPayloadSize(Headers headers, long payloadSize) {
    byte[] payloadSizeBytes = payloadSizeSerializer.serialize("dummy", payloadSize);
    headers.add(HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE, payloadSizeBytes);
  }
}
