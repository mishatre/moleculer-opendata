
import { Service as MoleculerService } from 'moleculer';
import { Service, Action, Method } from 'moleculer-decorators';
import axios from 'axios';
import path from 'path';
import fs from 'fs';
import parse from 'csv-parse';
import unzip from 'unzip-stream';
import { PassThrough, Transform } from 'stream';
import { pipeline } from 'stream/promises';
import iconvLite from 'iconv-lite';

interface OpenDataCatalog {
    [key: string]: any;
}

interface MedproductsRecord {
    unique_number: string;
    registration_number: string;
    registration_date: string;
    registration_date_end: string;
    name: string;
    applicant: string;
    applicant_address_post: string;
    applicant_address_legal: string;
    producer: string;
    producer_address_post: string;
    producer_address_legal: string;
    okp: string;
    class: string;
    appointment: string;
    kind: string;
    address_production: string;
    details: string;
}

@Service({
    name: 'opendata',
    version: 1,
})
export default class OpenDataService extends MoleculerService {

    @Action({
        name: 'loadOpendata',
        params: {
            opendataCatalog: 'string',
        }
    })
    public async requestCatalog() {



    }

    @Action({
        name: 'addOpenDataCatalog',
        params: {

        }
    })
    public async addCatalog() {

    }

    @Action({
        name: 'removeOpenDataCatalog',
        params: {
            opendataCatalog: 'string',
        }
    })
    public async removeCatalog() {

    }

    private async fetchMetaInfo(catalog: OpenDataCatalog) {

        const url = path.join(catalog.url, catalog.meta);

        try {

            const response = await axios.get(url);

            if (response.status === 200) {



            }

        } catch (error) {

        }

    }

    private async fetchData(catalog: OpenDataCatalog) {
        // const response = await axios.get('https://roszdravnadzor.gov.ru/opendata/7710537160-medproducts/data-20210919-structure-20150601.zip', {
        //     responseType: 'stream',
        // });

        const response = {
            data: fs.createReadStream('./data.zip')
        }

        // response.data.pipe(fs.createWriteStream('./data.zip'));
        // return;

        const parser = parse({
            columns: true,
            autoParseDate: true,
            delimiter: ';',
            relax: true,
        });
        // parser.on('data', console.log);

        await pipeline(
            response.data,
            unzip.Parse(),
            // @ts-ignore
            async function* (source: AsyncIterable<PassThrough>) {
                for await (const entry of source) {
                    for await (const chunk of entry.pipe(iconvLite.decodeStream('win1251')).pipe(parser) as AsyncIterable<MedproductsRecord>) {

                        const [name, content] = chunk.name.split('<br>');
                        const productionAddresses = chunk.address_production.split('\n');

                        const obj = {
                            uid: Number(chunk.unique_number),
                            number: chunk.registration_number,
                            issuedDate: chunk.registration_date,
                            validBefore: chunk.registration_date_end === '' ? null : chunk.registration_date_end,
                            name: name,
                            content,
                            applicant: chunk.applicant,
                            applicantLegalAddress: chunk.applicant_address_legal,
                            producer: chunk.producer,
                            producerLegalAddress: chunk.producer_address_legal,
                            okp: chunk.okp,
                            class: chunk.class,
                            kind: Number(chunk.kind),
                            productionAddresses,
                        }

                        yield obj;
                    }
                }
            },
            async function* (source: AsyncIterable<any>) {

                const s = new Set();

                for await (const chunk of source) {
                    if (!chunk.content) {
                        continue;
                    }
                    const content = (chunk.content as string);
                    if (content.includes(':')) {
                        const [first] = content.split(':');
                        s.add(first);
                    }

                    // break;
                }

                console.log([...s].join('\n'))

            }
        )

        // response.data
        //     .pipe(unzip.Parse())
        //     .pipe(
        //         new Transform({
        //             objectMode: true,
        //             transform: (entry, e, cb) => {
        //                 entry.pipe(iconvLite.decodeStream('win1251'))
        //                     .pipe(parser)
        //                     .pipe(new Transform({
        //                         objectMode: true,
        //                         transform: (entry, e, cb) => {
        //                             console.log(entry)
        //                         }
        //                     }));
        //                 // ).pipe(fs.createWriteStream('./test.csv')
        //                 // entry.pipe(
        //                 //     new Transform({
        //                 //         objectMode: false,
        //                 //         transform: (chunk, e, cb) => {
        //                 //             console.log(chunk.toString());

        //                 //         }
        //                 //     })
        //                 // )
        //                 // entry.pipe(fs.createWriteStream('./test.csv'))
        //                 cb();

        //                 // entry.pipe(parser)
        //                 //     .on('finish', cb);
        //             }
        //         })
        //     );

        // response.data.pipe(parser);

    }




    private started() {
        console.log('started')
        this.fetchData({});
        // this.catalog = {
        //     medinfo: {
        //         name: '',
        //         url: '',
        //         meta: {
        //             filename: '',
        //             ext: '',
        //         }
        //     }
        // }
    }

}