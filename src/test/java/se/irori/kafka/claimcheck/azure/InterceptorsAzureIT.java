package se.irori.kafka.claimcheck.azure;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys;

public class InterceptorsAzureIT extends AbstractClaimCheckIT {

  AzureBlobClaimCheckProducerInterceptor producerInterceptor;
  AzureBlobClaimCheckConsumerInterceptor consumerInterceptor;
  HashMap<String,Object> producerConfig;
  HashMap<String,Object> consumerConfig;

  private static final String TOPIC = "my-topic";

  @Rule
  public final AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);

  @Before
  public void setUp() {
    producerConfig = new HashMap<>();
    injectConfigFromSystemProperties(producerConfig, azuriteContainer, "producer.");

    producerConfig.putIfAbsent(
        Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG, 10);

    consumerConfig = new HashMap<>();
    injectConfigFromSystemProperties(consumerConfig, azuriteContainer, "consumer.");

    producerInterceptor = new AzureBlobClaimCheckProducerInterceptor();
    consumerInterceptor = new AzureBlobClaimCheckConsumerInterceptor();

    producerInterceptor.configure(producerConfig);
    consumerInterceptor.configure(consumerConfig);
  }


  @Test
  public void testProduceToBlob() {
    String key = "myKey";
    String value = "01234567890";
    ClaimCheck claimCheck = producerInterceptor.checkIn(
        new ProducerRecord<>(TOPIC, key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(
                StandardCharsets.UTF_8)));

    byte[] payloadBytes = consumerInterceptor.checkOut(claimCheck);
    String outPayload = new String(payloadBytes, StandardCharsets.UTF_8);
    assertEquals(value, outPayload);
  }

}