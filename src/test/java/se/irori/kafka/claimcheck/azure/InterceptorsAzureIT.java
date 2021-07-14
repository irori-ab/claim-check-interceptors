package se.irori.kafka.claimcheck.azure;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys;

public class InterceptorsAzureIT {

  AzureBlobClaimCheckProducerInterceptor producerInterceptor;
  AzureBlobClaimCheckConsumerInterceptor consumerInterceptor;
  HashMap<String,Object> baseConfig;

  private static final String TOPIC = "my-topic";

  private static final List<String> CONFIG_FROM_SYS_PROPS = Arrays.asList(
      Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG,
      Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG,
      Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG
  );

  @Rule
  public AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);


  @Before
  public void setUp() {
    Map<String, String> sysConfig = new HashMap<>();
    for (String k : CONFIG_FROM_SYS_PROPS) {
      String value = System.getProperty(k);
      if (value != null) {
        sysConfig.put(k, value);
      }
    }

    producerInterceptor = new AzureBlobClaimCheckProducerInterceptor();
    consumerInterceptor = new AzureBlobClaimCheckConsumerInterceptor();
    baseConfig = new HashMap<>(sysConfig);

    baseConfig.putIfAbsent(
        Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG, 10);

    if (!baseConfig.containsKey(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG) &&
          !baseConfig.containsKey(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG)) {
      // assume Azurite emulator default settings
      // https://github.com/Azure/Azurite#default-storage-account
      String token =
          "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
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
      baseConfig.put(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, connectionString);
    }

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