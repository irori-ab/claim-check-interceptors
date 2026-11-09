package se.irori.kafka.claimcheck.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig;
import se.irori.kafka.claimcheck.ClaimCheckSerializer;
import se.irori.kafka.claimcheck.ClaimCheckStreamingDeserializer;
import se.irori.kafka.claimcheck.ClaimCheckStreamingProducerInterceptor;
import se.irori.kafka.claimcheck.InputStreamSerializer;
import se.irori.kafka.claimcheck.TestUtils;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;

/**
 * Integration test on the KafkaProducer/KafkaConsumer level in streaming mode.
 *
 * <p>Kafka: a Testcontainers broker, or an external one if the system property
 * {@code kafka.bootstrap.servers} is set.
 *
 * <p>S3: an in-process S3Proxy, or an external S3 if the system property {@code s3.bucket} is set
 * (optionally with {@code s3.region} and {@code s3.endpoint.override}, credentials from the AWS
 * default provider chain).
 */
public class ProduceConsumeStreamingKafkaS3IT {

  private static final Logger LOG = LoggerFactory.getLogger(ProduceConsumeStreamingKafkaS3IT.class);

  private static final String TOPIC = "my-topic";

  private static KafkaContainer kafkaContainer;

  private static String bootstrapServers;

  @Rule
  public final S3ProxyRule s3Proxy = S3TestSupport.newS3Proxy();

  private KafkaProducer<String, InputStream> producer;
  private KafkaConsumer<String, InputStream> consumer;

  @BeforeClass
  public static void startKafka() {
    bootstrapServers = sysProp("kafka.bootstrap.servers");
    if (bootstrapServers == null) {
      // https://docs.confluent.io/platform/current/installation/versions-interoperability.html
      // 7.6.x => Kafka 3.6.0
      kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
      kafkaContainer.start();
      bootstrapServers = kafkaContainer.getBootstrapServers();
    } else {
      LOG.info("using external Kafka: {}", bootstrapServers);
    }
  }

  @AfterClass
  public static void stopKafka() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  private static String sysProp(String key) {
    String value = System.getProperty(key);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  private Map<String, Object> s3Config() {
    String bucket = sysProp(Keys.S3_BUCKET_CONFIG);
    if (bucket == null) {
      return S3TestSupport.s3Config(s3Proxy);
    }
    LOG.info("using external S3 bucket: {}", bucket);
    Map<String, Object> config = new HashMap<>();
    config.put(Keys.S3_BUCKET_CONFIG, bucket);
    for (String key : new String[] {Keys.S3_REGION_CONFIG, Keys.S3_ENDPOINT_OVERRIDE_CONFIG}) {
      if (sysProp(key) != null) {
        config.put(key, sysProp(key));
      }
    }
    return config;
  }

  @Before
  public void setUp() {
    Map<String, Object> producerConfig = new HashMap<>(s3Config());
    producerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        S3ClaimCheckBackend.class);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClaimCheckSerializer.class);
    producerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        InputStreamSerializer.class);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ClaimCheckStreamingProducerInterceptor.class.getName());

    Map<String, Object> consumerConfig = new HashMap<>(s3Config());
    consumerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        S3ClaimCheckBackend.class);
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ClaimCheckStreamingDeserializer.class);
    consumerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS,
        StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-" + UUID.randomUUID());

    producer = new KafkaProducer<>(producerConfig);
    consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(TOPIC));
  }

  @After
  public void tearDown() {
    producer.close();
    consumer.close();
  }

  @Test
  public void testKafkaProduceConsumeStreaming() throws ExecutionException, InterruptedException,
      IOException {
    String key = "myKey-" + UUID.randomUUID();
    String value = TestUtils.getRandomString(5 * 1024 * 1024);

    producer.send(TestUtils.streamRecordFromString(TOPIC, key, value)).get();

    List<ConsumerRecord<String, InputStream>> consumed = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 60_000;
    while (consumed.isEmpty() && System.currentTimeMillis() < deadline) {
      consumer.poll(Duration.ofSeconds(1)).forEach(r -> {
        if (key.equals(r.key())) {
          consumed.add(r);
        }
      });
    }
    assertEquals(1, consumed.size());

    ConsumerRecord<String, InputStream> record = consumed.get(0);
    assertNotNull("expected claim check header", record.headers().lastHeader(
        ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK));
    assertTrue(record.value() instanceof LazyS3InputStream);
    try (InputStream in = record.value()) {
      assertEquals(value, new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
