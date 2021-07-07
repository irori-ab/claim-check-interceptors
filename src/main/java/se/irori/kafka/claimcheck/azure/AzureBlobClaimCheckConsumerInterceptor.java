package se.irori.kafka.claimcheck.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.AbstractClaimCheckConsumerInterceptor;
import se.irori.kafka.claimcheck.ClaimCheck;

/**
 * Implementation of the ConsumerInterceptor backed by Azure Blob Storage.
 */
public class AzureBlobClaimCheckConsumerInterceptor extends AbstractClaimCheckConsumerInterceptor {
  public static final String AZURE_STORAGE_ACCOUNT_URL_CONFIG
      = "azure.blob.storage.account.url";

  // TODO: should be able to read secret from ENV or file?
  public static final String AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG
      = "azure.blob.storage.account.sastoken";

  public static final String AZURE_STORAGE_ACCOUNT_CONTAINER_CONFIG
      = "azure.blob.storage.account.container";

  // TODO: does this account for headers as well?
  private int maxInBandMessageUncompressedSize = 1048588;

  private String storageAccountUrl;

  private String storageAccountSasToken;

  private BlobServiceClient blobServiceClient;

  ConcurrentHashMap<String, BlobContainerClient> topicContainerClients = new ConcurrentHashMap<>();

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    String blobUrl = claimCheck.getReference();

    BlobUrlParts parts;
    try {
      parts = BlobUrlParts.parse(new URL(blobUrl));
    } catch (MalformedURLException e) {
      throw new KafkaStorageException("Bad Azure claim check url: " + blobUrl);
    }

    BlobClient blobClient = new BlobClientBuilder()
        .connectionString(storageAccountUrl)
        .containerName(parts.getBlobContainerName())
        .blobName(parts.getBlobName())
        .buildClient();
    BinaryData binaryData = blobClient.downloadContent();

    return binaryData.toBytes();
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

    blobServiceClient = new BlobServiceClientBuilder()
        .connectionString(storageAccountUrl)
        .buildClient();
  }
}
