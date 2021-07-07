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
import org.apache.kafka.common.config.ConfigException;
import se.irori.kafka.claimcheck.AbstractClaimCheckProducerInterceptor;
import se.irori.kafka.claimcheck.ClaimCheck;

/**
 * Implementation of the ProducerInterceptor backed by Azure Blob Storage.
 */
public class AzureBlobClaimCheckProducerInterceptor extends AbstractClaimCheckProducerInterceptor {
  public static final String AZURE_STORAGE_ACCOUNT_URL_CONFIG
      = "azure.blob.storage.account.url";

  // TODO: should be able to read secret from ENV or file?
  public static final String AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG
      = "azure.blob.storage.account.sastoken";

  public static final String AZURE_STORAGE_ACCOUNT_NAME_CONFIG
      = "azure.blob.storage.account.name";

  public static final String AZURE_STORAGE_ACCOUNT_CONTAINER_CONFIG
      = "azure.blob.storage.account.container";

  // TODO: does this account for headers as well?
  private int maxInBandMessageUncompressedSize = 1048588;

  private String storageAccountUrl;
  private String storageAccountSasToken;
  private String storageAccountName;

  private ConcurrentHashMap<String, BlobContainerClient> topicContainerClients
      = new ConcurrentHashMap<>();

  private BlobServiceClient blobServiceClient;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    BlobContainerClient blobContainerClient =
        topicContainerClients.computeIfAbsent(largeRecord.topic(),
            t -> blobServiceClient.getBlobContainerClient(t));

    blobContainerClient.create();

    String blobName = UUID.randomUUID().toString();
    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    blobClient.upload(new ByteArrayInputStream(largeRecord.value()), largeRecord.value().length);
    String blobUrl = blobClient.getBlobUrl();
    return new ClaimCheck(blobUrl);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object storageAccountUrlValue = configs.get(AZURE_STORAGE_ACCOUNT_URL_CONFIG);
    if (storageAccountUrlValue != null) {
      if (storageAccountUrlValue instanceof String) {
        storageAccountUrl = ((String) storageAccountUrlValue);
      } else {
        throw new ConfigException(AZURE_STORAGE_ACCOUNT_URL_CONFIG + " must be String");
      }
    } else {
      throw new ConfigException(AZURE_STORAGE_ACCOUNT_URL_CONFIG + " must be specified");
    }

    Object storageAccountSasTokenValue = configs.get(AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG);
    if (storageAccountSasTokenValue != null) {
      if (storageAccountSasTokenValue instanceof String) {
        storageAccountSasToken = ((String) storageAccountSasTokenValue);
      } else {
        throw new ConfigException(AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG + " must be String");
      }
    } else {
      throw new ConfigException(AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG + " must be specified");
    }

    BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder()
        .connectionString(storageAccountUrl);

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
