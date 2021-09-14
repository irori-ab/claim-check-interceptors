# Claim Check interceptors
Library implementing the Claim Check pattern for use with Kafka and Azure Blob Storage

## Overview design
![Claim check interceptor design diagram](/docs/claim-check-blob.png)

## Building 

`./mvnw clean install`

## Commits

Commit messages should follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.

## Testcontainers prerequisites
We use [testcontainers](https://www.testcontainers.org) for integration tests against Kafka and an
Azure Blob Storage emulator (Azurite). The tests are run with Maven Failsafe.

See [Docker pre-requisites](https://www.testcontainers.org/supported_docker_environment/)
for running these tests locally.

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
    
# write sas, +6 months expiry

# Producer: rcl
# (r) read
# (c) create
# (l) list

# TODO: how to restrict to specific container

az storage account generate-sas \
 --account-name claimcheckcitest \
 --permissions rcl \
 --services b \
 --resource-types co \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > my-topic-sas-write.sastoken 

az storage container generate-sas \
 --account-name claimcheckcitest \
 --permissions racwl \
 --name my-topic \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > my-topic-sas-write.sastoken 

# consumer: rl
# (r) read
# (l) list

# read sas, +6 months expiry
# TODO: how to restrict to specific container

az storage account generate-sas \
 --account-name claimcheckcitest \
 --permissions rl \
 --services b \
 --resource-types co \
 --https-only \
 --expiry $(date -v +6m +%Y-%m-%d) | tr -d '"' > my-topic-sas-read.sastoken 
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