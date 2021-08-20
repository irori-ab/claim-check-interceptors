package se.irori.kafka.claimcheck.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import se.irori.kafka.claimcheck.AbstractClaimCheckProducerInterceptor;
import se.irori.kafka.claimcheck.ClaimCheck;

/**
 * Implementation of the ProducerInterceptor backed by Azure Blob Storage.
 */
public class AzureBlobClaimCheckProducerInterceptor extends AbstractClaimCheckProducerInterceptor {

  private final ConcurrentHashMap<String, BlobContainerClient> topicContainerClients
      = new ConcurrentHashMap<>();

  private BlobServiceClient blobServiceClient;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    BlobContainerClient blobContainerClient =
        topicContainerClients.computeIfAbsent(largeRecord.topic(),
            t -> blobServiceClient.getBlobContainerClient(t));

    if (!blobContainerClient.exists()) {
      blobContainerClient.create();
    }

    String blobName = UUID.randomUUID().toString();
    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    blobClient.upload(new ByteArrayInputStream(largeRecord.value()), largeRecord.value().length);
    String blobUrl = blobClient.getBlobUrl();
    return new ClaimCheck(blobUrl);
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

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

  }

  @Override
  public void close() {

  }
}
