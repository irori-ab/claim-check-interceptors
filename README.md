# Claim Check interceptors
Library implementing the Claim Check pattern for use with Kafka and Azure Blob Storage

## Building 

`./mvnw clean install`

## Commits

Commit messages should follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.

## Testcontainers prerequisites
We use [testcontainers](https://www.testcontainers.org) for integration tests against Kafka and an
Azure Blob Storage emulator (Azurite). The tests are run with Maven Failsafe.

See [Docker pre-requisites](https://www.testcontainers.org/supported_docker_environment/)
for running these tests locally.

## Azure Blob Storage

### Setup storage account and credentials manually

```
az group create -l westeurope -n claimcheck-test

az storage account create \
    --resource-group claimcheck-test \
    --name claimchecktestsa \
    --location westeurope
    
# get the SA key, to create SAS tokens
az storage account keys list -g claimcheck-test -n claimchecktestsa --query '[0].value'
export AZURE_STORAGE_KEY=...
    
# write sas, +6 months expiry
az storage blob generate-sas \
    --account-name claimchecktestsa \
    --container-name my-topic \
    --name my-topic-sas-write-append \
    --permissions rac \
    --expiry $(date -v +6m +%Y-%m-%d) > my-topic-sas-write-append.sastoken 

# read sas, +6 months expiry
az storage blob generate-sas \
    --account-name claimchecktestsa \
    --container-name my-topic \
    --name my-topic-sas-read \
    --permissions r \
    --expiry $(date -v +6m +%Y-%m-%d) > my-topic-sas-read.sastoken
```

## Reference Documentation 

- [Azure SDK documentation](https://azuresdkartifacts.blob.core.windows.net/azure-sdk-for-java/index.html)