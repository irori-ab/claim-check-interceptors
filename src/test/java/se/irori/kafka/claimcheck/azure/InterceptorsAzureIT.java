package se.irori.kafka.claimcheck.azure;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.ClaimCheck;

public class InterceptorsAzureIT {

  AzureBlobClaimCheckProducerInterceptor producerInterceptor;
  AzureBlobClaimCheckConsumerInterceptor consumerInterceptor;
  HashMap<String,Object> baseConfig;

  private static final String TOPIC = "my-topic";

  @Rule
  public AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);

  @Before
  public void setUp() {

    producerInterceptor = new AzureBlobClaimCheckProducerInterceptor();
    consumerInterceptor = new AzureBlobClaimCheckConsumerInterceptor();
    baseConfig = new HashMap<>();

    baseConfig.put(
        AzureBlobClaimCheckProducerInterceptor.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG, 10);
    // https://github.com/Azure/Azurite#default-storage-account
    String token =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    baseConfig.put(AzureBlobClaimCheckProducerInterceptor.AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG,
        token);
    String host = "" + azuriteContainer.getContainerIpAddress();
    String account = "devstoreaccount1";
    // http://<local-machine-address>:<port>/<account-name>/<resource-path>
    String endpoint = String.format("http://%s:%d/%s/",
        host,
        azuriteContainer.getMappedPort(10000),
        account);
    String connectionString = "" +
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "BlobEndpoint=" + endpoint + ";" +
        "AccountKey=" + token + ";";
    baseConfig.put(AzureBlobClaimCheckProducerInterceptor.AZURE_STORAGE_ACCOUNT_URL_CONFIG,
        connectionString);
    producerInterceptor.configure(baseConfig);
    consumerInterceptor.configure(baseConfig);
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