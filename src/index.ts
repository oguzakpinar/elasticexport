import { writeFile, appendFile, readFile } from 'fs';
import axios from './../node_modules/axios';
declare var process: {
    argv: string[],
    stdout: any
};

export interface ExportOptions extends ImportOptions {
    query?: any;
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

export const exportData = (options: ExportOptions) => {
    options.host = checkHost(options.host);
    console.error(options);
    option = options;
    console.error(option);
    startExport();
}

export const importData = (options: ImportOptions) => {
    options.host = checkHost(options.host);
    console.error(options);
    option = options;
    startImport();
}

export const clearIndex = (options: DeleteOptions) => {
    options.host = checkHost(options.host);
    console.error(options);
    deleteIndex(options.host, options.index);
}

let runQuery = async (begin: number, query: any, resultSet: any[]) => {
    query.source.from = begin;
    query.source.size = 100;
    let path = option.host + '/' + option.index + '/_search/template';
    var res: any = await axios.post(path, JSON.stringify(query), {
        headers: {
            'Content-Type': 'application/json'
        }
    }).catch((err: Error) => console.log(err));
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

let writeToFile = (resultSet: any[]) => {
    console.log('Writing Progress');
    if (!option.path || !option.path.endsWith(".json")) {
        throw new Error('-path have to be given with .json format');
    }
    writeFile(option.path, '[', () => {
        console.log("File Created Successfully");
        for (let data of resultSet) {
            appendFile(option.path, (start ? '' : ',') + JSON.stringify(data), 'utf8', () => process.stdout.write(''));
            start = false;
        }
        appendFile(option.path, ']', 'utf8', () => console.log('\nExport Finish Successfully'));
    });
    let start = true;

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
    runQuery(0, requestQuey, resultSet).then(() => {
        writeToFile(resultSet);
    });
}

let readFromFile = () => {
    return new Promise<any[]>((resolve, reject) => {
        console.log('Reading Progress');
        if (!option.path || !option.path.endsWith(".json")) {
            throw new Error('-path have to be given with .json format');
        }
        readFile(option.path, 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                reject(err);
            } else {
                resolve(JSON.parse(data));
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
        console.log(idx);
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
    callBulkService(queryObjects).then(() => console.log('Completed Successfully'));
}

let callBulkService = async (objectList: any[], index = 0) => {
    let body = '';
    let toCnt = index + 50;
    for (let i = index; i < (toCnt > objectList.length ? objectList.length : toCnt); i++) {
        body = body + JSON.stringify(objectList[i]) + '\n';
    }
    try {
        await axios.post(option.host + '/_bulk', body, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        process.stdout.write("\r\x1b[K")
        process.stdout.write('Index ' + toCnt + ' of ' + objectList.length + 'data            ');
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
        console.error(err)
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
        }
    }

    if (isDelete) {
        clearIndex({ host: host, index: index });
    }

    if (isExport) {
        exportData({
            host: host,
            path: path,
            index: index,
            query: query
        });
    } else {
        importData({
            host: host,
            path: path,
            index: index
        });
    }
}