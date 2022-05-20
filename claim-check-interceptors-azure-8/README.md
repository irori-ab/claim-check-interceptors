# Azure interceptors for older SDK version
For compatibility with Spring 2.1.x and 2.2.x.

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
