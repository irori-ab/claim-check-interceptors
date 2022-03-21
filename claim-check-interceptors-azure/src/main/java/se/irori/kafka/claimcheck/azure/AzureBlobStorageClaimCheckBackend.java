package se.irori.kafka.claimcheck.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.ClaimCheckBackend;

/**
 * Implementation of the ClaimCheckBackend backed by Azure Blob Storage v12+ SDK.
 */
public class AzureBlobStorageClaimCheckBackend implements ClaimCheckBackend {

  private final ConcurrentHashMap<String, BlobContainerClient> topicContainerClients
      = new ConcurrentHashMap<>();

  private BlobServiceClient blobServiceClient;

  private boolean createContainerIfNotExists = false;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    BlobContainerClient blobContainerClient =
        topicContainerClients.computeIfAbsent(largeRecord.topic(),
            t -> blobServiceClient.getBlobContainerClient(t));

    if (createContainerIfNotExists && !blobContainerClient.exists()) {
      blobContainerClient.create();
    }

    String blobName = UUID.randomUUID().toString();
    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    blobClient.upload(new ByteArrayInputStream(largeRecord.value()), largeRecord.value().length);
    String blobUrl = blobClient.getBlobUrl();
    return new ClaimCheck(blobUrl);
  }

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

    createContainerIfNotExists = config.getBoolean(
        AzureClaimCheckConfig.Keys.AZURE_CREATE_CONTAINER_IF_NOT_EXISTS);
  }

}
