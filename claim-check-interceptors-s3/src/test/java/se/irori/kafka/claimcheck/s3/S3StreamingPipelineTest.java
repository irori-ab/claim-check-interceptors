package se.irori.kafka.claimcheck.s3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig;
import se.irori.kafka.claimcheck.ClaimCheckSerializer;
import se.irori.kafka.claimcheck.ClaimCheckStreamingDeserializer;
import se.irori.kafka.claimcheck.ClaimCheckStreamingProducerInterceptor;
import se.irori.kafka.claimcheck.InputStreamSerializer;
import se.irori.kafka.claimcheck.TestUtils;

/**
 * Runs the streaming interceptor, serializer and deserializer the way the Kafka clients would,
 * against an in-process S3Proxy, but without a Kafka broker.
 */
public class S3StreamingPipelineTest {

  private static final String TOPIC = "my-topic";

  @Rule
  public S3ProxyRule s3Proxy = S3TestSupport.newS3Proxy();

  private ClaimCheckStreamingProducerInterceptor<String> interceptor;
  private ClaimCheckSerializer<InputStream> serializer;
  private ClaimCheckStreamingDeserializer deserializer;

  @Before
  public void setUp() {
    Map<String, Object> producerConfig = new HashMap<>(S3TestSupport.s3Config(s3Proxy));
    producerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        S3ClaimCheckBackend.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClaimCheckSerializer.class);
    producerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        InputStreamSerializer.class);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    interceptor = new ClaimCheckStreamingProducerInterceptor<>();
    interceptor.configure(producerConfig);
    serializer = new ClaimCheckSerializer<>();
    serializer.configure(producerConfig, false);

    Map<String, Object> consumerConfig = new HashMap<>(S3TestSupport.s3Config(s3Proxy));
    consumerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        S3ClaimCheckBackend.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ClaimCheckStreamingDeserializer.class);
    consumerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS,
        StringDeserializer.class);
    deserializer = new ClaimCheckStreamingDeserializer();
    deserializer.configure(consumerConfig, false);
  }

  @After
  public void tearDown() {
    interceptor.close();
    serializer.close();
    deserializer.close();
  }

  private byte[] roundTrip(ProducerRecord<String, InputStream> record) throws IOException {
    ProducerRecord<String, InputStream> intercepted = interceptor.onSend(record);
    byte[] wireValue =
        serializer.serialize(TOPIC, intercepted.headers(), intercepted.value());

    try (InputStream consumed =
             deserializer.deserialize(TOPIC, intercepted.headers(), wireValue)) {
      return consumed.readAllBytes();
    }
  }

  @Test
  public void largePayloadIsCheckedInToS3() throws IOException {
    String value = TestUtils.getRandomString(3 * 1024 * 1024);
    ProducerRecord<String, InputStream> record =
        TestUtils.streamRecordFromString(TOPIC, "key", value);

    ProducerRecord<String, InputStream> intercepted = interceptor.onSend(record);
    assertNull("unexpected error header", intercepted.headers().lastHeader(
        ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_ERROR));
    assertNotNull(intercepted.headers().lastHeader(
        ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK));
    assertNull(intercepted.value());

    assertEquals(value, new String(roundTrip(
        TestUtils.streamRecordFromString(TOPIC, "key", value)), StandardCharsets.UTF_8));
  }

  @Test
  public void smallPayloadStaysInKafka() throws IOException {
    String value = "small";
    ProducerRecord<String, InputStream> record =
        TestUtils.streamRecordFromString(TOPIC, "key", value);

    ProducerRecord<String, InputStream> intercepted = interceptor.onSend(record);
    assertNull(intercepted.headers().lastHeader(
        ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK));

    assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), roundTrip(
        TestUtils.streamRecordFromString(TOPIC, "key", value)));
  }
}
