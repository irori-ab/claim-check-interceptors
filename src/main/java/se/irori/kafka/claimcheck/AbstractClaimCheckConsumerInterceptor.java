package se.irori.kafka.claimcheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

/**
 * Abstract implementation of the ClaimCheck pattern consumer side.
 */
public abstract class AbstractClaimCheckConsumerInterceptor
    implements ConsumerInterceptor<byte[], byte[]> {

  public abstract byte[] checkOut(ClaimCheck claimCheck);

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public ConsumerRecords<byte[], byte[]> onConsume(
      ConsumerRecords<byte[], byte[]> consumerRecords) {

    HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> newRecords = new HashMap<>();

    for (TopicPartition partition : consumerRecords.partitions()) {

      List<ConsumerRecord<byte[], byte[]>> partitionConsumerRecords = new ArrayList<>();
      for (ConsumerRecord<byte[], byte[]> record : consumerRecords.records(partition)) {
        if (isClaimCheck(record)) {
          ClaimCheck claimCheck = new ClaimCheck(record.value());
          byte[] value = this.checkOut(claimCheck);
          ConsumerRecord<byte[], byte[]> claimCheckRecord =
              new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                  record.timestamp(), record.timestampType(), record.checksum(),
                  record.serializedKeySize(), value.length, record.key(), value, record.headers(),
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

  /**
   * Check if record is a Claimcheck.
   */
  public boolean isClaimCheck(ConsumerRecord<byte[], byte[]> record) {
    boolean isClaimCheck = false;
    for (Header header : record.headers()) {
      if (AbstractClaimCheckProducerInterceptor.HEADER_NAME.equals(header.key())) {
        isClaimCheck = true;
      }
    }
    return isClaimCheck;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void close() {

  }
}
