import { TypedEmitter } from "tiny-typed-emitter";
import WalFile from "../files/Wal.js";
import SSTManager from "../managers/SST.js";
import { InMemBufferEvent, WalMethod } from "../typings/enum.js";
import { IColumnOptions } from "../typings/interface.js";
import { PossibleKeyType } from "../typings/type.js";
import { checksum } from "../utils/checksum.js";
import { ValueToDataType, ValueToTypedArray, getDataTypeByteLength } from "../utils/dataType.js";
import InMemoryBuffer from "./InMemBuffer.js";
import DataNode from "./Node.js";
import Cache from "./Cache.js";
import { BASE_BYTES } from "../files/SST.js";
import Table from "./Table.js";
import { mergeOptions } from "../utils/mergeOptions.js";
import fs from "node:fs";
export default class Column extends TypedEmitter {
	#options: IColumnOptions;
	#sstManager: SSTManager;
	#inMem: InMemoryBuffer;
	#kvLength: number;
	#wal: WalFile;
	#cache: Cache;
	#table: Table;
	constructor(options: IColumnOptions,table: Table) {
		super();

		this.#options = mergeOptions(Column.defaultOptions(), options);
		this.#sstManager = new SSTManager({...this.#options.sstConfig, path: this.#options.name, keyType: this.#options.keyType, valueType: this.#options.valueType},this);
		this.#inMem = new InMemoryBuffer(this.#options.memBufferConfig);
		this.#kvLength =
		BASE_BYTES +
			getDataTypeByteLength(this.#options.keyType) +
			getDataTypeByteLength(this.#options.valueType);
		this.#wal = new WalFile({...this.#options.walConfig, path: this.#options.path + '/log.wal'}, this);
		this.#cache = new Cache(this.#options.cacheSize);

		this.#inMem.on(InMemBufferEvent.NeedsFlush, async () => {
			await this.#flush();
			await this.#wal.truncate();
		});
		this.#table = table;
	}

	setPath(path: string) {
		this.#options.path = path;

		if(!fs.existsSync(path)){
			fs.mkdirSync(path, { recursive: true });
		}
		this.#sstManager.setPath(path);
		this.#wal.setPath(path + '/log.wal');
	}

	static defaultOptions(): IColumnOptions {
		return {
			name: "default",
			valueType: "u32",
			keyType: "u32",
			sstConfig: SSTManager.defaultOptions(),
			memBufferConfig: {
				threshHold: 100000,
			},
			walConfig: WalFile.defaultOptions(),
			cacheSize: 10000,
			path: "./data",
		};
	
	}

	async #flush() {
		const data = this.#inMem.flush();
		await this.#sstManager.flushToDisk(data);
	}

	async #syncLogs() {
		const logs = await this.#wal.read(true);
		for (const log of logs) {
			this.#inMem.insert(log);
			if (this.#inMem.isLocked()) {
				await this.#flush();
				await this.#wal.truncate();
			}
		}
	}

	get options() {
		return this.#options;
	}

	async init() {
		await this.#sstManager.init();
		await this.#wal.open();
		await this.#syncLogs();
	}

	async readLogs() {
		return await this.#wal.read(false);
	}

	insert(key: PossibleKeyType, value: unknown) {
		const keyBuffer = ValueToDataType(key, this.#options.keyType);
		const valueBuffer = ValueToDataType(value, this.#options.valueType);
		const dataBuffer = new Uint8Array(keyBuffer.length + valueBuffer.length);

		dataBuffer.set(keyBuffer);
		dataBuffer.set(valueBuffer, keyBuffer.length);

		const checksumBuffer = checksum(dataBuffer);

		const node = new DataNode({
			key: key,
			keyType: this.#options.keyType,
			value: value,
			valueType: this.#options.valueType,
			offset: -1,
			checksum: checksumBuffer,
			delete: false,
			length: this.#kvLength,
			timestamp: Date.now(),
			dataBuffer: dataBuffer,
		});
		this.#inMem.insert(node);
		this.#cache.set(key, node);
		this.#wal.append(node, WalMethod.Insert);
	}

	async get(key: PossibleKeyType) {
		// check in memory
		if(this.#inMem.has(key)){
			return this.#inMem.get(key);
		} 
		// check in cache
		if(this.#cache.has(key)){
			return this.#cache.get(key);
		}

		// check in SST
		return await this.#sstManager.get(key);
	}

	mayHasKey(key: PossibleKeyType) {
		return this.#inMem.has(key) || this.#cache.has(key) || this.#sstManager.mayHasKey(key);
	}

	async has(key: PossibleKeyType) {
		return (
			this.#inMem.has(key) ||
			this.#cache.has(key) ||
			((await this.#sstManager.hasKey(key)) ?? false)
		);
	}

	async delete(key: PossibleKeyType) {
		// check key exists
		if (!this.mayHasKey(key)) {
			return false;
		}
		// this part can be skipped cause we are just merging the data in sst later on
		// const node = await this.get(key);
		// if (node === null) {
		// 	return false;
		// }

		const newNode = DataNode.deletedNode(
			key,
			this.#options.keyType,
			this.#options.valueType
		);

		this.#inMem.insert(newNode);
		this.#cache.delete(key);
		this.#wal.append(newNode, WalMethod.Delete);
	}

	async close() {
		await this.#sstManager.close();
		await this.#wal.close();
	}

	async clear() {
		await this.#sstManager.clear();
		await this.#wal.truncate();
		this.#inMem.clear();
	}
    
	async stats() {
		return {
			sst: await this.#sstManager.stats(),
			wal: this.#wal.stats(),
			inMem: this.#inMem.stats(),
		};
	}

	async ping() {
		return this.#sstManager.ping();
	}

	get wal() {
		return this.#wal;
	}

	get inMem() {
		return this.#inMem;
	}

	get table() {
		return this.#table;
	}
}
