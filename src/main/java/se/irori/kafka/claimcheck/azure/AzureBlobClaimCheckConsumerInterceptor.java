package se.irori.kafka.claimcheck.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.AbstractClaimCheckConsumerInterceptor;
import se.irori.kafka.claimcheck.ClaimCheck;

/**
 * Implementation of the ConsumerInterceptor backed by Azure Blob Storage.
 */
public class AzureBlobClaimCheckConsumerInterceptor extends AbstractClaimCheckConsumerInterceptor {

  private BlobServiceClient blobServiceClient;

  private final ConcurrentHashMap<String, BlobContainerClient> topicContainerClients
      = new ConcurrentHashMap<>();

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    String blobUrl = claimCheck.getReference();

    BlobUrlParts parts;
    try {
      parts = BlobUrlParts.parse(new URL(blobUrl));
    } catch (MalformedURLException e) {
      throw new KafkaStorageException("Bad Azure claim check url: " + blobUrl);
    }

    BlobContainerClient blobContainerClient =
        topicContainerClients.computeIfAbsent(parts.getBlobContainerName(),
            t -> blobServiceClient.getBlobContainerClient(t));

    BlobClient blobClient = blobContainerClient.getBlobClient(parts.getBlobName());

    BinaryData binaryData = blobClient.downloadContent();

    return binaryData.toBytes();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    AzureClaimCheckConfig config = AzureClaimCheckConfig.validatedConfig(configs);
    String connectionString =
        config.getString(AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG);
    final BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder();
    if (connectionString != null) {
      blobServiceClientBuilder
          .connectionString(connectionString);
    } else {
      blobServiceClientBuilder
          .sasToken(config.getSasToken())
          .endpoint(config.getString(
              AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG));
    }

    blobServiceClient = blobServiceClientBuilder
        .buildClient();
  }
}
