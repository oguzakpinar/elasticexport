import { exists, readFile } from "fs";

export class ReadData {
    public static async readFromFile(path: string, suffix = 0) {
        return new Promise<any[]>((resolve, reject) => {
            let pathValue = path;
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
                            let res: any = JSON.parse(data);
                            if(res instanceof Array) {
                                let resultSet: any[] = res;
                                console.log(resultSet.length + ' data found in ' + pathValue);
                                this.readFromFile(path, suffix + 1).then((value) => {
                                    if (value) {
                                        resultSet = resultSet.concat(value);
                                    }
                                    resolve(resultSet);
                                });
                            } else {
                                resolve(res);
                            }
                        }
                    });
                } else {
                    resolve();
                }
            });
        });
    }
}