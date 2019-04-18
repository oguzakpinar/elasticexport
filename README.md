# ElasticSearch Dump Operations For version-6

Required Node.js for work.
```sh
$ npm install -g elasticexport
```

## Capabilities
### Export Data
Options
  - -host http:// is not required host and port is required 
  - -path Output file for export operation .json extension is required
  - export is required to process as export mode 
  - -index is not required. If it is added export specific index otherwise export all indexes
  - -fileSize default 4MB. If written data more than 4MB next data wtire in different file. 
Example usage
```sh
    elasticexport export -host localhost:9200 -index test -path /Users/xxx/Desktop/test.json -fileSize 4096
```

### Import Data
Options
  - -host http:// is not required host and port is required 
  - -path Inpot file for export operation .json extension is required. If *path*`n`.json file exists program read sequentially
  - import is required to process as import mode 
  - -index is not required. If it is added import data to specific index otherwise if export data runs with `all` options import process with same indexes, otherwise error occured
Example usage
```sh
    elasticexport import -host localhost:9200 -index test_document -path /Users/xxx/Desktop/test.json 
```

### Clear Index
Options
  - -host http:// is not required host and port is required 
  - delete is required to process as import mode 
  - -index is cleared index name
Example usage
```sh
    elasticexport delete -host localhost:9200 -index test 
```

Also user import and delete both it works first delete given index than import all data in also given index
```sh
elasticexport delete import -host localhost:9200 -index test_document -path /Users/xxx/Desktop/test.json
```
## Module Usage
Node projects use these options as module. In type script 
```ts
import { exportData, importData, clearIndex } from 'elasticexport';
exportData({
    host: 'localhost:9200',
    path: '/Users/xxx/Desktop/test.json',
    index: 'test'
});
clearIndex({
    host: 'localhost:9200',
    index: 'test2'
});
importData({
    host: 'localhost:9200',
    path: '/Users/xxx/Desktop/test.json',
    index: 'test2'
});
```