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

let option: ExportOptions;

export const exportData = (options: ExportOptions) => {
    console.error(options);
    option = options;
    console.error(option);
    startExport();
}

export const importData = (options: ImportOptions) => {
    console.error(options);
    option = options;
    console.error(option);
    startImport();
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
}

if (process.argv.length > 2) {
    let host: string = '';
    let index: string = '';
    let path: string = '';
    let query: string = '';
    let isExport: boolean = true;
    for (let i = 0; i < process.argv.length; i++) {
        switch (process.argv[i]) {
            case '-host':
                host = process.argv[i + 1].toString();
                break;
            case '-index':
                index = process.argv[i + 1].toString();
                break;
            case 'import':
                isExport = false;
                break;
            case 'export':
                isExport = true;
                break;
            case '-path':
                path = process.argv[i + 1].toString();
                break;
            case '-query':
                query = process.argv[i + 1];
                break;
        }
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