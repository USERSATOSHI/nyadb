import { get4pointRangeOfDataType } from "../../dist/utils/dataType.js";
import {
	CompressionFlag,
	EncodingFlag,
} from "../../dist/typings/enum.js";
import { setTimeout } from "node:timers/promises";
import v8 from "node:v8";
import Column from "../../dist/structs/Column.js";
import { inspect } from "node:util";
const range = get4pointRangeOfDataType("u32");
const keys = [];

const column = new Column({
	keyType: "u32",
	valueType: "u32",
	memBufferConfig: {
		threshHold: 20000,
	},
	walConfig: {
		path: "./b/column/txt.wal",
		maxSize: 20000,
		maxBufferSize:100
	},
	name: "test",
	sstConfig: {
		growthFactor: 10,
		keyType: "u32",
		valueType: "u32",
		path: "./b/column",
		readMmap: true,
		levels: 5,
		threadsForMerge:8,
		sstThreshold: 20000,
		sstConfig: {
			compression: CompressionFlag.None,
			encoding: EncodingFlag.None,
			keyDataType: "u32",
			kvCount: 20000,
			kvPerPage: 1000,
			doBatchValidation: false,
		},
	},
	cacheSize: 10000,
});

await column.init();

function getRandomData() {
	const key = Math.floor(Math.random() * range[3]);
	keys.push(key);
	return [
		key,
		Math.floor(Math.random() * range[3]),
	];
}

function getRandom() {
	return Math.floor(Math.random() * range[3]);
}

// generate 200k random keys and values
const data = new Array(200000).fill(0).map(() => getRandomData());

const times = {
	insert: 0,
	get: 0,
	has: 0,
	mayHas: 0,
}

const ops = (time) => 200000*1000/time;

const tpo = (time) => time/200000;

const resultTable = {};


function runInsert200k() {
	const time = performance.now();
	for (let i = 0; i < 200000; i++) {
		column.insert(data[i][0], data[i][1]);
	}
	times.insert = performance.now() - time;
}

async function runGet200k() {
	const time = performance.now();
	for (let i = 0; i < 200000; i++) {
		await column.get(keys[Math.floor(Math.random() * keys.length)]);
	}
	times.get = performance.now() - time;
}

async function runHas200k() {
	const time = performance.now();
	for (let i = 0; i < 200000; i++) {
		await column.has(getRandom());
	}
	times.has = performance.now() - time;
}

function runMayHas200k() {
	const time = performance.now();
	for (let i = 0; i < 200000; i++) {
		column.mayHasKey(getRandom());
	}
	times.mayHas = performance.now() - time;
}

runInsert200k();
await runGet200k();
await runHas200k();
runMayHas200k();

resultTable.insert = {
	totalTime: times.insert,
	ops: ops(times.insert),
	tpo: tpo(times.insert),
}

resultTable.get = {
	totalTime: times.get,
	ops: ops(times.get),
	tpo: tpo(times.get),
}

resultTable.has = {
	totalTime: times.has,
	ops: ops(times.has),
	tpo: tpo(times.has),
}

resultTable.mayHas = {
	totalTime: times.mayHas,
	ops: ops(times.mayHas),
	tpo: tpo(times.mayHas),
}

console.table(resultTable);
await setTimeout(5000);
console.log(inspect(await column.stats(),{
	depth: 10,
}));
