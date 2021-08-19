package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import se.irori.kafka.claimcheck.azure.AzureBlobClaimCheckConsumerInterceptor;
import se.irori.kafka.claimcheck.azure.BaseClaimCheckConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of the ClaimCheck pattern consumer side.
 */
public class DeserializingClaimCheckConsumerInterceptor<K, V>
    implements ConsumerInterceptor<K, V> {

  private Deserializer<K> keyDeserializer;

  private Deserializer<V> valueDeserializer;

  private AbstractClaimCheckConsumerInterceptor claimCheckConsumerInterceptor;

  @Override
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    Deserializer valueConfiguredInstance = baseClaimCheckConfig
            .getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    this.valueDeserializer = valueConfiguredInstance;

    Deserializer keyConfiguredInstance = baseClaimCheckConfig
            .getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    this.keyDeserializer = keyConfiguredInstance;

    // TODO: make it generic abstract ProducerInterceptor
    claimCheckConsumerInterceptor = new AzureBlobClaimCheckConsumerInterceptor();
    claimCheckConsumerInterceptor.configure(configs);
  }

  // we (probably) need to propagate the deprecated checksum field
  @SuppressWarnings("deprecation")
  @Override
  public ConsumerRecords<K, V> onConsume(
      ConsumerRecords<K, V> consumerRecords) {

    HashMap<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();

    for (TopicPartition partition : consumerRecords.partitions()) {

      List<ConsumerRecord<K, V>> partitionConsumerRecords = new ArrayList<>();
      for (ConsumerRecord<K, V> record : consumerRecords.records(partition)) {
        if (isClaimCheck(record)) {
          ClaimCheck claimCheck = new ClaimCheck(getClaimCheckRefFromHeader(record));
          V value = valueDeserializer.deserialize(record.topic(), record.headers(), checkOut(claimCheck));

          ConsumerRecord<K, V> claimCheckRecord =
              new ConsumerRecord<K, V>(record.topic(), record.partition(), record.offset(),
                  record.timestamp(), record.timestampType(), record.checksum(),
                  record.serializedKeySize(), record.serializedKeySize(), record.key(), value, record.headers(),
                  record.leaderEpoch());
          partitionConsumerRecords.add(claimCheckRecord);
        } else {
          partitionConsumerRecords.add(record);
        }
      }
      newRecords.put(partition, partitionConsumerRecords);

    }
    return new ConsumerRecords<>(newRecords);
  }

  public byte[] checkOut(ClaimCheck claimCheck) {
    return claimCheckConsumerInterceptor.checkOut(claimCheck);
  }

  /**
   * Check if record is a Claimcheck.
   */
  public boolean isClaimCheck(ConsumerRecord<K, V> record) {
    boolean isClaimCheck = false;
    for (Header header : record.headers()) {
      if (AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK
          .equals(header.key())) {
        isClaimCheck = true;
      }
    }
    return isClaimCheck;
  }

  /**
   * Get claim check reference from header
   * @param record
   */
  public byte[] getClaimCheckRefFromHeader(ConsumerRecord<K, V> record) {
    byte[] ret = null;
    for (Header header : record.headers()) {
      if (AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK
          .equals(header.key())) {
        ret = header.value();
      }
    }
    return ret;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void close() {

  }
}
