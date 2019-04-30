import { ExportOptions, ImportOptions } from ".";
import Axios from "axios";
import { writeFile } from "fs";
import { ReadData } from "./read-data";

export class MappingOperation {
    public static async exportData(option: ExportOptions) {
        if (!option.host) {
            throw new Error('-host have to be given');
        }
        let resultSet: any[] = [];
        console.log("Retriewing Strarted");
        let path = option.host + '/' + option.index + '/_settings';
        var res: any = await Axios.get(path);
        res = res.data;
        let indexSettings: any = {};
        for (let idx in res) {
            if (res[idx]) {
                indexSettings[idx] = {
                    settings: {
                        index: {
                        }
                    },
                    mappings: {
                    }
                };
                for (let prop in res[idx].settings.index) {
                    if (res[idx].settings.index[prop] && ["version", "uuid", "provided_name", "creation_date"].indexOf(prop) < 0) {
                        indexSettings[idx].settings.index[prop] = res[idx].settings.index[prop];
                    }
                }
            }
        }

        path = option.host + '/' + option.index + '/_mapping';
        res = await Axios.get(path);
        res = res.data;
        for (let idx in res) {
            if (res[idx] && indexSettings[idx]) {
                for (let prop in res[idx].mappings) {
                    if (res[idx].mappings[prop]) {
                        indexSettings[idx].mappings[prop] = res[idx].mappings[prop];
                    }
                }
            }
        }
        await this.writeMapping(option, indexSettings);
    }

    private static writeMapping(option: ExportOptions, data: any) {
        return new Promise<any>((resolve, reject) => {
            if (!option.path || !option.path.endsWith(".json")) {
                reject('-path have to be given with .json format');
            }
            writeFile(option.path, JSON.stringify(data), 'utf8', () => {
                console.log('Export Finish Successfully');
                resolve();
            });
        })
    }

    private static async checkIndex(host: string, index: string) {
        let exists = await Axios.head(host + '/' + index, {
            validateStatus: (number) => true
        });
        if (exists.status === 200) {
            await Axios.delete(host + '/' + index)
        }
    }

    public static async importData(option: ImportOptions) {
        try {
            if (!option.host) {
                throw new Error('-host have to be given');
            }
            let result: any = await ReadData.readFromFile(option.path);
            if (option.index && this.checkSize(result) === 1) {
                result = this.updateName(result, option.index);
            }
            for (let idx in result) {
                if (result[idx]) {
                    await this.checkIndex(option.host, idx);
                    let path = option.host + '/' + idx;
                    console.log(path);
                    await Axios.put(path, JSON.stringify(result[idx]), {
                        headers: {
                            'Content-Type': 'application/json'
                        }
                    }).catch(err => console.error(err));
                }
            }
            console.log("Import Completed");
        } catch (err) {
            console.log(err);
        }
    }

    private static checkSize(object: any) {
        let cnt = 0;
        for (let idx in object) {
            if (object[idx]) {
                cnt++;
            }
        }
        return cnt;
    }

    private static updateName(object: any, newName: string) {
        let result: any = {};
        for (let idx in object) {
            if (object[idx]) {
                result[newName] = object[idx];
            }
        }
        return result;
    }
}