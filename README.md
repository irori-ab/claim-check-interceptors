# Claim Check interceptors
Library implementing the Claim Check pattern for use with Kafka and Azure Blob Storage

## Overview design
![Claim check interceptor design diagram](/docs/claim-check-blob.png)

[Detailed component diagram](/docs/claim-check-blob-detailed.png)

## Usage
Example usage for the `claim-check-interceptors-azure` (Azure SDK v12) backend. See also the 
[Note about Azure SDK versions](claim-check-interceptors-azure).

Add the dependency:
```
<dependency>
  <groupId>se.irori.kafka</groupId>
  <artifactId>claim-check-interceptors-azure</artifactId>
  <version>0.7.1-SNAPSHOT</version>
</dependency>
```

Configure your Kafka consumer/producer properties:
```
// common
config.put(
    BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
    AzureBlobStorageClaimCheckBackend.class);
config.put(
    AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG,
    "https://MY_STORAGE_ACCOUNT_NAME.blob.core.windows.net");    
config.put(
    AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG,
    "file:/path/to/textfile/with/sas.token");   

// producer (interceptor + wrapping serializer)
// any serializer as the wrapped serializer
config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClaimCheckSerializer.class);
config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
    ClaimCheckProducerInterceptor.class.getName());
config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        StringSerializer.class);
    
// consumer (wrapping deserializer)
// any deserializer as the wrapped serializer
config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ClaimCheckDeserializer.class);
config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS,
        StringDeserializer.class);
```

## Config reference

`claimcheck.backend.class`
The fully qualified name of the backend implementation. E.g. `se.irori.kafka.claimcheck.azure.AzureBlobStorageClaimCheckBackend`

* Type: class
* Importance: medium

`claimcheck.checkin.uncompressed-batch-size.over.bytes`
The the byte limit where Kafka record batches above this size are checked in using the Claim Check backend. *Note*: this applies to the uncompressed message batch size. If you want to optimize for more messages  not being checked in when compression is used, you will need to experiment with  compression ratios for your specific flow, and then increase this config.

* Type: long
* Default: 1048064
* Importance: medium

`interceptor.classes`
Set to `se.irori.kafka.claimcheck.ClaimCheckProducerInterceptor` for the Producer in a Claim Check enabled flow.

* Type: list
* Default: null
* Importance: medium

`key.serializer`
Standard Kafka key.serializer option. Used  for the calculation of message size to determine if it should be checked in.

* Type: class
* Default: null
* Importance: medium

`value.deserializer`
Set to `se.irori.kafka.claimcheck.ClaimCheckDeserializer` for the Consumer in a Claim Check enabled flow.

* Type: class
* Default: null
* Importance: medium

`value.deserializer.wrapped.deserializer`
Set to the normal Kafka Consumer de-serializer that would have been used before enabling Claim Check interceptors on the flow.

* Type: class
* Default: null
* Importance: medium

`value.serializer`
Set to `se.irori.kafka.claimcheck.ClaimCheckSerializer` for the Producer in a Claim Check enabled flow.

* Type: class
* Default: null
* Importance: medium

`value.serializer.wrapped.serializer`
Set to the normal Kafka Producer serializer that would have been used before enabling Claim Check interceptors on the flow.

* Type: class
* Default: null
* Importance: medium

See additional config reference per backend:
* [Azure v12 backend](claim-check-interceptors-azure/README.md)
* [Azure v8 backend](claim-check-interceptors-azure-8/README.md)

## Building 

`./mvnw clean install`

## Commits

Commit messages should follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.

## Testcontainers prerequisites
We use [testcontainers](https://www.testcontainers.org) for integration tests against Kafka and an
Azure Blob Storage emulator (Azurite). The tests are run with Maven Failsafe.

See [Docker pre-requisites](https://www.testcontainers.org/supported_docker_environment/)
for running these tests locally.

## Backends

### Azure Blob Storage v12
See [docs](./claim-check-interceptors-azure/README.md).

### Azure Blob Storage v8
See [docs](./claim-check-interceptors-azure-8/README.md).

## Run integration tests

### Azurite
With `azurite` Azure API emulator: `mvn verify -Pazurite`.


### Azure
With real Azure storage account, and SAS tokens stored as local files: `mvn verify -Pazure`. (see next section to setup).

If using another storage account than the example, you can override the endpoint the command:
```
 mvn verify -Pazure -Dproducer.azure.blob.storage.account.endpoint=https://???.blob.core.windows.net/ \
   -Dconsumer.azure.blob.storage.account.endpoint=https://???.blob.core.windows.net/
```

You will need two files in the project root: `my-topic-sas-read.sastoken` and `my-topic-sas-write.sastoken`.

## Azure Blob Storage

### Setup storage account and credentials manually

```
az group create -l westeurope -n claimcheckrg

az storage account create \
    --resource-group claimcheckrg \
    --name claimcheckcitest \
    --location westeurope
    
# get the SA key, to create SAS tokens
az storage account keys list -g claimcheckrg -n claimcheckcitest --query '[0].value'
export AZURE_STORAGE_KEY=...
    
# for container specific sas tokens the container (topic) needs to be created
az storage container create --name my-topic --account-name claimcheckcitest --resource-group claimcheckrg
    
# container (topic) restricted write sas, +6 months expiry

# Producer: rcl
# (r) read
# (c) create
# (l) list

az storage container generate-sas \
 --account-name claimcheckcitest \
 --permissions rcl \
 --name my-topic \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > my-topic-sas-write.sastoken 

# container (topic) restricted read sas, +6 months expiry
# consumer: rl
# (r) read
# (l) list

az storage container generate-sas \
 --account-name claimcheckcitest \
 --permissions rl \
 --name my-topic \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > my-topic-sas-read.sastoken 
```

Note: if you want to use the create container if not exists feature, then you need general storage account 
write permission, not tied to a specific container, e.g.;
```
az storage account generate-sas \
 --account-name claimcheckcitest \
 --permissions rwlac \
 --services b \
 --resource-types co \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > general-write.sastoken 
```

## Set Blob expiry
The following sets a Storage Account Lifecycle Management policy that will delete blobs after 14 days:
```
cat << EOF > example-expiry-policy-14-days.json
{
  "rules": [
    {
      "enabled": true,
      "name": "expire-claim-check-messages",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "baseBlob": {
            "delete": {
              "daysAfterModificationGreaterThan": 14
            }
          }
        },
        "filters": {
          "blobTypes": [
            "blockBlob"
          ]
        }
      }
    }
  ]
}
EOF

az storage account management-policy create --account-name myaccount --policy @example-expiry-policy-14-days.json --resource-group myresourcegroup
```

## Reference Documentation

- [Azure SDK documentation](https://azuresdkartifacts.blob.core.windows.net/azure-sdk-for-java/index.html)
- [SAS token structure](https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas)
- [Azure Blob Management Policies CLI actions](https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview)
- [Azure Blob Management Policies](https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview)
