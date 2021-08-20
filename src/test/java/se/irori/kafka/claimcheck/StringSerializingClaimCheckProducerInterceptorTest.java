package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class StringSerializingClaimCheckProducerInterceptorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringSerializingClaimCheckProducerInterceptorTest.class);

  DummySerializingClaimCheckProducerInterceptor unit;

  @Before
  public void setup() {
    unit = new DummySerializingClaimCheckProducerInterceptor(); // reset counter
    HashMap<String, Object> config = new HashMap<>();
    config.put(
        AzureClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG, 10);
    config.put(
            AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(
            AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "value:testSasToken");
    config.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    unit.configure(config);
  }

  @Test
  public void onSendLargeString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body > 10 bytes
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    "01234567890");
    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    Header headerResult = result.headers().iterator().next();

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0", new ClaimCheck(headerResult.value()).getReference());
    assertEquals(AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK,
        headerResult.key());
    assertEquals(1, unit.getCount());
  }

  @Test
  public void onSendSmallString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body < 10 bytes
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    "0123456789");

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0123456789", result.value());
    assertEquals(0, unit.getCount());
  }

  @Test
  public void onSendNullString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    String nullStr = new String();
    // WHEN sending a record with null key, and body < 10 bytes
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    nullStr);

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals(nullStr, result.value());
    assertEquals(0, unit.getCount());
  }

  public static class DummySerializingClaimCheckProducerInterceptor<K, V>
      extends SerializingClaimCheckProducerInterceptor<K, V> {

    private int counter = 0;

    public int getCount() {
      return counter;
    }

    @Override
    public ClaimCheck claimCheck(ProducerRecord<byte[], byte[]> largeRecord) {
      return new ClaimCheck((counter++) + "");
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }
  }
}