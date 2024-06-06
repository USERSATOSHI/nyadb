import fsp from "fs/promises";
import b from "benny";
import SSTFile from "../../dist/files/SST.js";
import { get4pointRangeOfDataType } from "../../dist/utils/dataType.js";
import { BloomFilterType, CompressionFlag, EncodingFlag } from "../../dist/typings/enum.js";
import DataNode from "../../dist/structs/Node.js";
import { checksum } from "../../dist/utils/checksum.js";
import { setTimeout } from "node:timers/promises";
import v8 from 'node:v8'

const range = get4pointRangeOfDataType("u32");

const file = new SSTFile({
	path: "./b/sst/test.sst",
	compression: CompressionFlag.None,
	encoding: EncodingFlag.None,
	keyDataType: "u32",
	dataType: "u32",
	max: range[3],
	min: range[0],
	bloomFilter: BloomFilterType.Classic,
	kvCount: 50000,
	// doBatchValidation: true,
});

await file.open();

console.log(file.metaData, file.headerData);

class InMemoryBuffer {
	#options;
	#buffers = [];
	#lock = false;
	#currentSize = 0;
	constructor(options) {
		this.#options = options;
	}

	get options() {
		return this.#options;
	}

	get threshHold() {
		return this.#options.threshHold;
	}

	set threshHold(value) {
		this.#options.threshHold = value;
	}

	insert(data) {
		if (this.#lock) {
			throw new Error("Buffer is locked");
		}
		if(!data) {
			return ;
		}
		this.#buffers.push(data);
		this.#currentSize += data.length;

		if (this.#currentSize >= this.#options.threshHold) {
			this.#lock = true;
		}
	}

	flush() {
		this.#lock = false;
		const data = this.#buffers;
		this.#buffers = [];
		this.#currentSize = 0;
		return data;
	}

	isLocked() {
		return this.#lock;
	}
}
const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const getRandomStr512Key = () => {
	let result = "";
	for (let i = 0; i < 512; i++) {
		result += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return result;
};

const getRandom32UintValue = () => {
	return Math.floor(Math.random() * 4294967295);
};

const inMem = new InMemoryBuffer({
	threshHold: 50000 * file.metaData.kvPairLength.total,
});

let key = 0;
let value = 0;

let idx = 13;
function getRandomData() {
	const keybuffer = new Uint32Array(1);
	const valuebuffer = new Uint32Array(1);

	keybuffer[0] = getRandom32UintValue();
	valuebuffer[0] = value;

	const checksumdatabuffer = new Uint8Array(keybuffer.byteLength + valuebuffer.byteLength);
	checksumdatabuffer.set(new Uint8Array(keybuffer.buffer));
	checksumdatabuffer.set(new Uint8Array(valuebuffer.buffer), keybuffer.byteLength);
	const check = checksum(checksumdatabuffer);


	const node = new DataNode({
		key: key,
		value: value,
		checksum: check,
		delete: false,
		keyType: "u32",
		valueType: "u32",
		offset: idx,
		timestamp: BigInt(Date.now()),
		length: file.metaData.kvPairLength.total,
	});
	idx += node.toUint8Array().length;
	key++;
	value++;
	return node.toUint8Array();
}

let i = 0;
await setTimeout(1000);

async function run() {
	let times = 1;

	while (times--) {
		await b.suite(
			"SST TEST",
			b.add("SST Insert", async () => {
				const node = getRandomData();
				inMem.insert(node);

				if (inMem.isLocked()) {
					// console.log("Flushing");
					const d = inMem.flush();
					await file.write(d);
				}
			}),
			b.add("SST Read from file", async () => {
				await file.readKey(Math.floor(Math.random() * getRandom32UintValue()));
			}),
			b.add("SST Read from Mmap", async () => {
				console.log((await file.readKeyMmap(Math.floor(Math.random() * getRandom32UintValue())))?.build());
			}),
			b.add("sst Read OptiMMap", async () => {
				(await file.optreadKeyMmap(Math.floor(Math.random() * getRandom32UintValue())));
			}),
			b.add("SST HasKey", async () => {
				await file.hasKey(Math.floor(Math.random() * getRandom32UintValue()),true);
			}),

			b.add("SST May has Key", () => {
				file.mayHasKey(Math.floor(Math.random() * getRandom32UintValue()));
			}),
			b.cycle(),
			b.complete(),
			b.save({ file: "SST", version: "1.0.0" }),
			b.save({ file: "SST", version: "1.0.0", format: "chart.html" })
		);


	}
}
await run();
console.log(v8.getHeapStatistics())
await file.close();