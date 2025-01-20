## Read Me

### Description

* This backend system has 2 lambdas

--- 1. APIHandler Lambda  -> Which gets triggered by the API gateway when a user uploads a photo and begins the infringement report.

--- 2. PROCESSOR_LAMBDA -> Which gets triggered by the APIHandler Lanmbda and processes the Search, compare.






#### Below are the windows commands used for zipping and shipping both lambda files
```
Compress-Archive -Path .\unifiedHandler.js, .\utils.js, .\node_modules\* -DestinationPath unifiedHandler.zip -Force
```


```
Compress-Archive -Path .\handler.js, .\utils.js, .\node_modules\* -DestinationPath Handler.zip -Force
```