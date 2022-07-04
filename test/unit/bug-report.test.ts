/**
 * this is a template for a test.
 * If you found a bug, edit this test to reproduce it
 * and than make a pull-request with that failing test.
 * The maintainer will later move your test to the correct possition in the test-suite.
 *
 * To run this test do:
 * - 'npm run test:node' so it runs in nodejs
 * - 'npm run test:browser' so it runs in the browser
 */
// import assert from 'assert';
// import AsyncTestUtil from 'async-test-util';
import config from './config';

import { createRxDatabase, randomCouchString } from '../../';
import { getRxStorageLoki } from '../../plugins/lokijs';

const COLLECTIONS_COUNT = 100;

console.log('### bug report test ###');

describe('bug-report.test.js', () => {
    it('should fail because it reproduces the bug', async () => {
        console.log('### bug report test ### 1');

        /**
         * If your test should only run in nodejs or only run in the browser,
         * you should comment in the return operator and addapt the if statement.
         */
        if (
            config.platform.isNode() // runs only in node
            // config.platform.isNode() // runs only in the browser
        ) {
            return;
        }

        // create a schema

        const mySchema = {
            version: 0,
            primaryKey: 'passportId',
            type: 'object',
            properties: {
                passportId: {
                    type: 'string',
                    maxLength: 100,
                },
                firstName: {
                    type: 'string',
                },
                lastName: {
                    type: 'string',
                },
                age: {
                    type: 'integer',
                    minimum: 0,
                    maximum: 150,
                },
            },
        };

        // generate a random database-name
        const name = randomCouchString(10);

        // create a database
        const db = await createRxDatabase({
            name,
            /**
             * By calling config.storage.getStorage(),
             * we can ensure that all variations of RxStorage are tested in the CI.
             */
            storage: getRxStorageLoki(),
            eventReduce: true,
            ignoreDuplicate: true,
        });
        // create a collection

        // create
        const collectionCreators = new Array(COLLECTIONS_COUNT)
            .fill(null)
            .reduce((acc, current, idx) => {
                acc['mycollection' + idx] = {
                    schema: mySchema,
                };

                return acc;
            }, {});

        await db.addCollections(collectionCreators);

        await Promise.all(
            Object.keys(db.collections).map((collName) => {
                return db.collections[collName].bulkInsert(
                    new Array(COLLECTIONS_COUNT)
                        .fill({
                            passportId: 'foobar',
                            firstName: 'Bob',
                            lastName: 'Kelso',
                            age: 56,
                        })
                        .map((d, idx) => {
                            const cpy = { ...d };
                            cpy.passportId = cpy.passportId + idx;
                            return cpy;
                        })
                );
            })
        );

        await Promise.all(
            Object.keys(db.collections).map((collName) => {
                return db.collections[collName].remove();
            })
        );

        // clean up afterwards
        db.destroy();
    });
});
