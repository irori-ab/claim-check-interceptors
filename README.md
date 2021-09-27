# Claim Check interceptors
Library implementing the Claim Check pattern for use with Kafka and Azure Blob Storage

## Overview design
![Claim check interceptor design diagram](/docs/claim-check-blob.png)

## Using
Add the dependency:
```
<dependency>
  <groupId>se.irori.kafka</groupId>
  <artifactId>claim-check-interceptors</artifactId>
  <version>1.0-SNAPSHOT</version>
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

// producer
// any serializer
config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
    SerializingClaimCheckProducerInterceptor.class.getName());
    
// consumer 
// any deserializer
config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
config.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
    DeserializingClaimCheckConsumerInterceptor.class.getName());
```

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

### Azure Blob Storage
See [docs](./claim-check-interceptors-azure/README.md).
