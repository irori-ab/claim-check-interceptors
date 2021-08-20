package se.irori.kafka.claimcheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import se.irori.kafka.claimcheck.azure.AzureBlobClaimCheckConsumerInterceptor;
import se.irori.kafka.claimcheck.azure.BaseClaimCheckConfig;

/**
 * Abstract implementation of the ClaimCheck pattern consumer side.
 */
public class DeserializingClaimCheckConsumerInterceptor<K, V>
    implements ConsumerInterceptor<K, V> {

  private Deserializer<V> valueDeserializer;

  private AbstractClaimCheckConsumerInterceptor claimCheckConsumerInterceptor;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    this.valueDeserializer = baseClaimCheckConfig.getConfiguredInstance(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);

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
          V value = valueDeserializer
              .deserialize(record.topic(), record.headers(), checkOut(claimCheck));

          ConsumerRecord<K, V> claimCheckRecord =
              new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                  record.timestamp(), record.timestampType(), record.checksum(),
                  record.serializedKeySize(),
                  record.serializedKeySize(),
                  record.key(),
                  value,
                  record.headers(),
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
   * Get claim check reference from header.
   *
   * @param record the record to process headers for
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
