import b from "benny";
import { get4pointRangeOfDataType } from "../../dist/utils/dataType.js";
import {
	CompressionFlag,
	EncodingFlag,
} from "../../dist/typings/enum.js";
import { setTimeout } from "node:timers/promises";
import v8 from "node:v8";
import Column from "../../dist/structs/Column.js";
const range = get4pointRangeOfDataType("u32");
const keys = [];

const column = new Column({
	keyType: "u32",
	valueType: "u32",
	memBufferConfig: {
		threshHold: 100000,
	},
	path: "./b/column",
	walConfig: {
		path: "./b/column/txt.wal",
		maxSize: 100000,
		maxBufferSize:1000
	},
	name: "test",
	sstConfig: {
		growthFactor: 100,
		keyType: "u32",
		valueType: "u32",
		path: "./b/column",
		readMmap: true,
		levels: 5,
		threadsForMerge:8,
		sstThreshold: 100000,
		sstConfig: {
			compression: CompressionFlag.None,
			encoding: EncodingFlag.None,
			keyDataType: "u32",
			kvCount: 100000,
			kvPerPage: 1000,
			doBatchValidation: false,
		},
	},
	cacheSize: 10000,
});

await column.init();

function getRandomData() {
	return [
		Math.floor(Math.random() * range[3]),
		Math.floor(Math.random() * range[3]),
	];
}

function getRandom() {
	return Math.floor(Math.random() * range[3]);
}

await setTimeout(5000);
const avg = [];
async function run() {
	let times = 1;

	while (times--) {
		await b.suite(
			"Column",
			b.add("insert",() => {
				const data = getRandomData();
				keys.push(data[0]);
				column.insert(data[0], data[1]);
			}),

			b.add("get", async () => {
				await column.get(keys[Math.floor(Math.random() * keys.length)]);
			}),
			b.add("has", async () => {
				await column.has(keys[Math.floor(Math.random() * keys.length)]);
			}),
			b.add("mayHas", () => {
				column.mayHasKey(keys[Math.floor(Math.random() * keys.length)]);
			}),
			// b.add("delete", async () => {
			// 	await column.delete(getRandom());
			// }),
			b.cycle(),
			b.complete(),
			b.save({ file: "SST", version: "1.0.0" }),
			b.save({ file: "SST", version: "1.0.0", format: "chart.html" })
		);
	}
}

await run();



console.log(Object.entries(v8.getHeapStatistics()).map(([key, value]) => `${key}: ${(value / 1024 / 1024).toFixed(2)} MB`));

console.log((await column.wal.read(false)).length);
console.log(column.inMem.size);

console.log(await column.stats());
