package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import se.irori.kafka.claimcheck.azure.AzureBlobClaimCheckProducerInterceptor;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;
import se.irori.kafka.claimcheck.azure.BaseClaimCheckConfig;

import java.util.Map;

/**
 * Abstract implementation of the ClaimCheck pattern producer side.
 */
public class SerializingClaimCheckProducerInterceptor<K, V>
    implements ProducerInterceptor<K, V> {

  public static final String HEADER_MESSAGE_CLAIM_CHECK
      = "message-claim-check";

  // TODO: does this account for headers as well?
  private long checkinUncompressedSizeOverBytes = 1048588;

  private Serializer<K> keySerializer;

  private Serializer<V> valueSerializer;

  private AbstractClaimCheckProducerInterceptor claimCheckProducerInterceptor;


  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    checkinUncompressedSizeOverBytes = baseClaimCheckConfig.getLong(
      AzureClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG);

    Serializer valueConfiguredInstance = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
    this.valueSerializer = valueConfiguredInstance;

    Serializer keyConfiguredInstance = baseClaimCheckConfig
            .getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    this.keySerializer = keyConfiguredInstance;

    // TODO: make it generic abstract ProducerInterceptor
    claimCheckProducerInterceptor = new AzureBlobClaimCheckProducerInterceptor();
    claimCheckProducerInterceptor.configure(configs);
  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    long headerTotalSize = 0;

    for (Header header : producerRecord.headers()) {
      headerTotalSize += header.key().length() + header.value().length;
    }

    final long keySize;
    if (producerRecord.key() == null) {
      keySize = 0;
    } else {
      byte[] keyBytes = keySerializer.serialize(producerRecord.topic(), producerRecord.headers(), producerRecord.key());
      keySize = keyBytes.length;
    }

    final long valueSize;
    byte[] valueBytes = null;
    if (producerRecord.value() == null) {
      valueSize = 0;
    } else {
      valueBytes = valueSerializer.serialize(producerRecord.topic(), producerRecord.headers(), producerRecord.value());
      valueSize = valueBytes.length;
    }

    if (keySize + valueSize + headerTotalSize > checkinUncompressedSizeOverBytes) {
      ClaimCheck claimCheck = claimCheckProducerInterceptor
              .checkIn(new ProducerRecord<byte[], byte[]>(producerRecord.topic(),
                      producerRecord.partition(),
                      producerRecord.timestamp(),
                      null,
                      valueBytes,
                      null));
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

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    claimCheckProducerInterceptor.onAcknowledgement(recordMetadata, e);
  }

  @Override
  public void close() {
    claimCheckProducerInterceptor.close();
    keySerializer.close();
    valueSerializer.close();
  }
}
