package se.irori.kafka.claimcheck.azurev8;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.ClaimCheckBackend;

/**
 * Implementation of the ProducerInterceptor backed by Azure Blob Storage using SDK version v8.
 *
 * <p>This backend is needed to support a wider range of runtime environments, e.g. Spring Boot
 * v2.1 and v2.2. It seems newer versions of the SDK tend to cause issues with "projectreactor.io"
 * dependencies, unless running very specific combinations.
 */
public class AzureBlobStorageClaimCheckBackendV8 implements ClaimCheckBackend {

  private final ConcurrentHashMap<String, CloudBlobContainer> topicContainerClients
      = new ConcurrentHashMap<>();

  private CloudBlobClient blobServiceClient;

  private boolean createContainerIfNotExists = false;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    final CloudBlockBlob blockBlobReference =
        getMessageBlob(largeRecord.topic());


    try {
      blockBlobReference.uploadFromByteArray(largeRecord.value(), 0, largeRecord.value().length);
    } catch (Exception e) {
      throw new KafkaStorageException(e);
    }

    return new ClaimCheck(blockBlobReference.getUri().toString());
  }

  private CloudBlockBlob getMessageBlob(String topic) {
    CloudBlobContainer blobContainerClient =
        getCloudBlobContainer(topic);

    if (createContainerIfNotExists) {
      try {
        blobContainerClient.createIfNotExists();
      } catch (StorageException e) {
        throw new KafkaStorageException(e);
      }
    }

    String blobName = UUID.randomUUID().toString();
    final CloudBlockBlob blockBlobReference;
    try {
      blockBlobReference = blobContainerClient.getBlockBlobReference(blobName);
    } catch (Exception e) {
      throw new KafkaStorageException(e);
    }
    return blockBlobReference;
  }

  @Override
  public ClaimCheck checkInStreaming(String topic, InputStream payload, long payloadSize) {
    final CloudBlockBlob blockBlobReference = getMessageBlob(topic);

    try {
      blockBlobReference.upload(payload, payloadSize);
    } catch (Exception e) {
      throw new KafkaStorageException(e);
    }

    return new ClaimCheck(blockBlobReference.getUri().toString());
  }

  private CloudBlobContainer getCloudBlobContainer(String topic) {

    return topicContainerClients.computeIfAbsent(topic, t -> {
      try {
        return blobServiceClient.getContainerReference(t);
      } catch (Exception e) {
        throw new KafkaStorageException(e);
      }
    });
  }

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    final CloudBlockBlob blob = getBlobFromClaimCheck(claimCheck);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      blob.download(byteArrayOutputStream);
    } catch (StorageException e) {
      throw new KafkaStorageException(e);
    }

    return byteArrayOutputStream.toByteArray();
  }

  private CloudBlockBlob getBlobFromClaimCheck(ClaimCheck claimCheck) {
    String blobUrl = claimCheck.getReference();

    final CloudBlockBlob blob;
    try {
      blob = new CloudBlockBlob(new URI(blobUrl), blobServiceClient.getCredentials());
    } catch (URISyntaxException | StorageException | RuntimeException e) {
      throw new KafkaStorageException("Bad Azure claim check url: " + blobUrl);
    }
    return blob;
  }

  @Override
  public InputStream checkOutStreaming(ClaimCheck claimCheck) {
    final CloudBlockBlob blob = getBlobFromClaimCheck(claimCheck);

    try {
      return blob.openInputStream();
    } catch (StorageException e) {
      throw new KafkaStorageException(e);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    AzureClaimCheckConfig config = AzureClaimCheckConfig.validatedConfig(configs);

    String connectionString =
        config.getString(AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG);

    if (connectionString == null) {
      String sasToken = config.getSasToken();
      String endpoint =
          config.getString(AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG);

      // assume default endpoint format
      // http://mystorageaccount.blob.core.windows.net
      // https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#storage-accounts
      final String account;
      try {
        URI uri = new URI(endpoint);
        String host = uri.getHost();

        account = host.split("\\.")[0];
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
      connectionString = String.format(
          "DefaultEndpointsProtocol=https;"
              + "AccountName=%s;"
              + "SharedAccessSignature=%s;"
              + "BlobEndpoint=%s;",
          account, sasToken, endpoint
      );
    }

    final CloudStorageAccount cloudStorageAccount;
    try {
      cloudStorageAccount = CloudStorageAccount.parse(connectionString);
    } catch (Exception e) {
      throw new ConfigException("Bad Azure Blob Storage connection string", e);
    }

    blobServiceClient = cloudStorageAccount.createCloudBlobClient();

    createContainerIfNotExists = config.getBoolean(
        AzureClaimCheckConfig.Keys.AZURE_CREATE_CONTAINER_IF_NOT_EXISTS);
  }
}
