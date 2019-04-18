import { writeFile, appendFile, readFile, exists } from 'fs';
import axios from './../node_modules/axios';
declare var process: {
    argv: string[],
    stdout: any
};

export interface ExportOptions extends ImportOptions {
    query?: any;
    maxFileSize?: number;
}

export interface ImportOptions {
    host: string;
    path: string;
    index?: string;
}

export interface DeleteOptions {
    host: string;
    index: string;
}

let option: ExportOptions;

export const exportData = async (options: ExportOptions) => {
    options.host = checkHost(options.host);
    option = options;
    console.log(option);
    await startExport();
}

export const importData = async (options: ImportOptions) => {
    options.host = checkHost(options.host);
    option = options;
    console.log(option);
    await startImport();
}

export const clearIndex = async (options: DeleteOptions) => {
    options.host = checkHost(options.host);
    console.log(options);
    await deleteIndex(options.host, options.index);
}

let runQuery = async (begin: number, query: any, resultSet: any[]) => {
    query.source.from = begin;
    query.source.size = 100;
    let path = option.host + '/' + option.index + '/_search/template';
    var res: any = await axios.post(path, JSON.stringify(query), {
        headers: {
            'Content-Type': 'application/json'
        }
    }).catch((err: Error) => {
        console.error(err);
        throw err;
    });
    res = res.data;
    begin = begin + 100;
    for (let data of res.hits.hits) {
        if (option.index !== '_all') {
            data = data._source;
        }
        resultSet.push(data);
    }
    process.stdout.write("\r\x1b[K")
    process.stdout.write('Reteave ' + resultSet.length + ' of ' + res.hits.total + 'data            ');
    if (res.hits.total > begin) {
        await runQuery(begin, query, resultSet);
    } else {
        console.log('\nRetriewing Completed')
    }
}

let writeToFile = (resultSet: any[], lastAdded = 0, suffix = 0) => {
    return new Promise<any>((resolve, reject) => {
        if (!option.path || !option.path.endsWith(".json")) {
            throw new Error('-path have to be given with .json format');
        }
        let pathValue = option.path;
        if (suffix > 0) {
            pathValue = pathValue.replace('.json', suffix + '.json');
        }
        let writeSize = 0;
        let completed = true;
        let start: any;
        writeFile(pathValue, '[', () => {
            for (let i = lastAdded; i < resultSet.length; i++) {
                let data = JSON.stringify(resultSet[i]);
                appendFile(pathValue, (!start ? '' : ',') + data, 'utf8', () => process.stdout.write(''));
                start = true;
                writeSize = writeSize + data.length;
                let maxSize = (option.maxFileSize ? option.maxFileSize : 4096) * 1024;
                if (writeSize > maxSize) {
                    writeToFile(resultSet, i + 1, suffix + 1).then(() => resolve(true));
                    completed = false;
                    break;
                }
            }
            appendFile(pathValue, ']', 'utf8', () => {
                if (completed) {
                    console.log('Export Finish Successfully');
                    resolve(true);
                }
            });
        });
    });

}

let startExport = async () => {
    if (!option.host) {
        throw new Error('-host have to be given');
    }
    let requestQuey: any = {};
    if (option.query) {
        let q = JSON.parse(option.query);
        if (!q.query) {
            throw new Error('-query contains query object');
        }
        requestQuey.source = q;
    } else {
        requestQuey.source = {
            query: {
                match_all: {}
            }
        };
    }
    let resultSet: any[] = [];
    console.log("Retriewing Strarted");
    await runQuery(0, requestQuey, resultSet);
    console.log('Writing Progress');
    await writeToFile(resultSet);
}

let readFromFile = () => {
    return new Promise<any[]>((resolve, reject) => {
        console.log('Reading Progress');
        if (!option.path || !option.path.endsWith(".json")) {
            throw new Error('-path have to be given with .json format');
        }
        readOperation().then(data => {
            resolve(data);
        })
    });
}

let readOperation = (suffix = 0) => {
    return new Promise<any[]>((resolve, reject) => {
        let pathValue = option.path;
        if (suffix > 0) {
            pathValue = pathValue.replace('.json', suffix + '.json');
        }
        exists(pathValue, exists => {
            if (exists) {
                readFile(pathValue, 'utf8', (err, data) => {
                    if (err) {
                        console.error(err);
                        reject(err);
                    } else {
                        let resultSet: any[] = JSON.parse(data);
                        console.log(resultSet.length + ' data found in ' + pathValue);
                        readOperation(suffix + 1).then((value) => {
                            if (value) {
                                resultSet = resultSet.concat(value);
                            }
                            resolve(resultSet);
                        });
                    }
                });
            } else {
                resolve();
            }
        });
    });
}

let startImport = async () => {
    if (!option.host) {
        throw new Error('-host have to be given');
    }
    let resultSet: any[] = await readFromFile();
    console.log('ResultSet Completed with ' + resultSet.length + ' records.');
    let indexedData: any = checkDataImportable(resultSet);
    let queryObjects: any[] = [];
    for (let idx in indexedData) {
        if (indexedData[idx]) {
            for (let data of indexedData[idx]) {
                queryObjects.push({
                    index: {
                        _index: idx,
                        _type: '_doc'
                    }
                });
                queryObjects.push(data);
            }
        }
    }
    await callBulkService(queryObjects).then(() => console.log('\nCompleted Successfully'));
}

let callBulkService = async (objectList: any[], index = 0) => {
    let body = '';
    let toCnt = index + 100;
    toCnt = toCnt > objectList.length ? objectList.length : toCnt;
    for (let i = index; i < toCnt; i++) {
        body = body + JSON.stringify(objectList[i]) + '\n';
    }
    try {
        await axios.post(option.host + '/_bulk', body, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        process.stdout.write("\r\x1b[K")
        process.stdout.write('Index ' + (toCnt / 2) + ' of ' + (objectList.length / 2) + ' data            ');
        if (toCnt < objectList.length) {
            await callBulkService(objectList, toCnt);
        }
    } catch (err) {
        console.error(err);
        throw err;
    }
}

let deleteIndex = async (host: string, index: string) => {
    try {
        await axios.delete(host + '/' + index, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        console.log('Delete Operation Completed');
    } catch (err) {
        console.error(err);
        throw err;
    }
}

let checkDataImportable = (resultSet: any[]) => {
    let indexedMap: any = {};
    if (option.index && option.index.length > 0) {
        let row = resultSet[0];
        if (row._index && row._source) {
            indexedMap[option.index] = [];
            for (let d of resultSet) {
                indexedMap[option.index].push(d._source);
            }
        } else {
            indexedMap[option.index] = resultSet;
        }
    } else {
        let row = resultSet[0];
        if (row._index && row._source) {
            for (let d of resultSet) {
                if (!indexedMap[d._index]) {
                    indexedMap[d._index] = [];
                }
                indexedMap[d._index].push(d._source);
            }
        } else {
            throw new Error('Imported and given data has no index parameter.');
        }
    }
    return indexedMap;
}

let checkHost = (host: string) => {
    if (host.endsWith('/')) {
        host = host.substring(0, host.length - 1);
    }
    if (!host.startsWith('http://') && !host.startsWith('https://')) {
        host = 'http://' + host;
    }
    return host;
}

if (process.argv.length > 2) {
    let host: string = '';
    let index: string = '';
    let path: string = '';
    let query: string = '';
    let fileSize: number = 4096;
    let isExport: any = null;
    let isDelete: boolean = false;
    for (let i = 0; i < process.argv.length; i++) {
        switch (process.argv[i]) {
            case '-host':
                host = checkHost(process.argv[i + 1].toString());
                break;
            case '-index':
                index = process.argv[i + 1].toString();
                break;
            case 'import':
                if (isExport === true) {
                    throw new Error('Import and export cannot use at the same time');
                }
                isExport = false;
                break;
            case 'export':
                if (isExport === false) {
                    throw new Error('Import and export cannot use at the same time');
                }
                isExport = true;
                break;
            case 'delete':
                isDelete = true;
                break;
            case '-path':
                path = process.argv[i + 1].toString();
                break;
            case '-query':
                query = process.argv[i + 1];
                break;
            case '-fileSize':
                fileSize = parseInt(process.argv[i + 1]);
                break;

        }
    }

    if (isDelete) {
        clearIndex({ host: host, index: index }).then(() => {
            if (isExport) {
                exportData({
                    host: host,
                    path: path,
                    index: index,
                    query: query,
                    maxFileSize: fileSize
                });
            } else {
                importData({
                    host: host,
                    path: path,
                    index: index
                });
            }
        });
    } else {
        if (isExport) {
            exportData({
                host: host,
                path: path,
                index: index,
                query: query,
                maxFileSize: fileSize
            });
        } else {
            importData({
                host: host,
                path: path,
                index: index
            });
        }
    }
}