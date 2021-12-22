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
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of the ClaimCheck pattern consumer side.
 */
public class DeserializingClaimCheckConsumerInterceptor<K, V>
    implements ConsumerInterceptor<K, V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeserializingClaimCheckConsumerInterceptor.class);

  private Deserializer<V> valueDeserializer;

  private ClaimCheckBackend claimCheckBackend;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    this.valueDeserializer = baseClaimCheckConfig.getConfiguredInstance(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    this.valueDeserializer.configure(configs, false);

    this.claimCheckBackend = baseClaimCheckConfig.getConfiguredInstance(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ClaimCheckBackend.class);
  }

  // we (probably) need to propagate the deprecated checksum field
  @SuppressWarnings("deprecation")
  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {

    HashMap<TopicPartition, List<ConsumerRecord<K, V>>> newRecords = new HashMap<>();

    for (TopicPartition partition : consumerRecords.partitions()) {

      List<ConsumerRecord<K, V>> partitionConsumerRecords = new ArrayList<>();
      for (ConsumerRecord<K, V> record : consumerRecords.records(partition)) {
        if (ClaimCheckUtils.isClaimCheck(record.headers())) {

          ClaimCheck claimCheck =
              new ClaimCheck(ClaimCheckUtils.getClaimCheckRefFromHeader(record.headers()));
          LOG.trace("received claim check: topic={}, partition={}, offset={}, key={}, ref={}",
              record.topic(), record.partition(), record.offset(), record.key(),
              claimCheck.getReference());
          V value = valueDeserializer
              .deserialize(record.topic(), record.headers(), checkOut(claimCheck));
          LOG.trace("checked out claim check: ref={}", claimCheck.getReference());
          ConsumerRecord<K, V> claimCheckRecord =
              new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                  record.timestamp(), record.timestampType(), null,
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
    return claimCheckBackend.checkOut(claimCheck);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void close() {

  }
}
