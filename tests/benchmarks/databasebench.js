import b from 'benny';

import Database from '../../dist/structs/Database.js';
import Column from '../../dist/structs/Column.js';
import { get4pointRangeOfDataType } from "../../dist/utils/dataType.js";

const range = get4pointRangeOfDataType('u32');
const keyrange = get4pointRangeOfDataType('u64');
const keys = [];
const money = new Column({
	name: 'money',
	keyType: 'u64',
	valueType: 'u32',
	cacheSize: 10000,
});

const db = new Database({
	name: 'test',
	path: './b/database',
	tables: [{
		name: 'main',
		columns: [money],
	}]
});

function randomAsciiNumString(length) {
	const asciiNums = '0123456789';
	let result = '';
	for (let i = 0; i < length; i++) {
		result += asciiNums[Math.floor(Math.random() * asciiNums.length)]
	}
	return result;

}
function getRandomData() {
	// random bigint
	const key = BigInt(randomAsciiNumString(18));
	keys.push(key);
	return [
		// generate a random bigint
		key,
		Math.floor(Math.random() * range[3]),
	];
}

//generate 2m keys
console.time("generate 2m random keys");
const data = [];
for (let i = 0; i < 2000000; i++) {
	data.push(getRandomData());
}
console.timeEnd("generate 2m random keys");

function getRandom() {
	return keys[Math.floor(Math.random() * keys.length)];
}

await db.init();
let idx = 0;
await b.suite(
	'Database',
	b.add('Insert', () => {
		db.insert('main', {
			column: 'money',
			key: data[idx][0],
			value: data[idx][1],
		});
		idx++;
	}),
	b.add('Get', async () => {
		await db.get('main', {
			column: 'money',
			key: getRandom(),
		});
	}),
	b.add('Has', async () => {
		await db.has('main', {
			column: 'money',
			key: getRandom(),
		});
	}),
	b.add('bloomCheck', () => {
		db.bloomCheck('main', {
			column: 'money',
			key: getRandom(),
		});
	}),
    b.cycle(),
	b.complete(),
	b.save({ file: 'database' , version: '1.0.0' }),
	b.save({ file: 'database' , version: '1.0.0', format: 'chart.html' }),
	b.save({ file: 'database' , version: '1.0.0', format: 'json' }),
);

await db.close();
