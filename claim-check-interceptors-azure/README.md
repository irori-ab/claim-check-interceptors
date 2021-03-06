# Azure Blob Storage claim check Backend

## Note about Azure SDK versions
The Azure SDK has varying compatibility with different Java frameworks in different versions.

See this [note](https://github.com/Azure/azure-storage-java/blob/master/V12%20Upgrade%20Story.md) about the SDK
version evolution. It seems that we can cover most runtime environments by supporting v8 and v12+ of the SDK.

Prefer to use the `claim-check-interceptors-azure` if it works with your runtime environment. Otherwise
you can try `claim-check-interceptors-azure-v8`, which e.g. seems to work well with Spring Boot 2.1 and 2.2.

If you encounter dependency issues with the v12+ version of the SDK related to JSON serialization or HTTP
clients, you can try working with the pluggable mechanisms in the SDK:
- [Custom JSON serializer](https://github.com/Azure/azure-sdk-for-java/wiki/Custom-JSON-serializer)
- [Custom HTTP client](https://github.com/Azure/azure-sdk-for-java/wiki/Custom-HTTP-Clients)


## Config reference

`azure.blob.create.container.if.not.exists`
Create the container if it does not exist. *Note* this seems to require a SAS token with full account access. This does not work well with SAS tokens limited to a specific container (topic)

* Type: boolean
* Default: false
* Importance: medium

`azure.blob.storage.account.connectionstring`
Configure a connection string to connect to the storage account. See [docs](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)

* Type: string
* Default: null
* Importance: medium

`azure.blob.storage.account.endpoint`
Storage account service endpoint. Either use this in combination with `azure.blob.storage.account.sastoken.from` or instead configure `azure.blob.storage.account.connectionstring`

* Type: string
* Default: null
* Importance: medium

`azure.blob.storage.account.sastoken.from`
Where to fetch the SAS token credential from. Valid mechanisms are `path:`, `env:`, or `value:`. Use *path* for specifying a path to a file on disk. Use *env* for fetching from an enviroment variable. Or use *value* to specify the SAS token value explicitly.

* Type: string
* Default: null
* Importance: medium

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