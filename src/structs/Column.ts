import { TypedEmitter } from "tiny-typed-emitter";
import WalFile from "../files/Wal.js";
import SSTManager from "../managers/SST.js";
import { InMemBufferEvent, WalMethod } from "../typings/enum.js";
import { IColumnOptions } from "../typings/interface.js";
import { PossibleKeyType } from "../typings/type.js";
import { checksum } from "../utils/checksum.js";
import { ValueToTypedArray, getDataTypeByteLength } from "../utils/dataType.js";
import InMemoryBuffer from "./InMemBuffer.js";
import DataNode from "./Node.js";

export default class Column extends TypedEmitter {
	#options: IColumnOptions;
	#sstManager: SSTManager;
	#inMem: InMemoryBuffer;
	#kvLength: number;
	#wal: WalFile;

	constructor(options: IColumnOptions) {
		super();
		this.#options = options;
		this.#sstManager = new SSTManager(this.#options.sstConfig);
		this.#inMem = new InMemoryBuffer(this.#options.memBufferConfig);
		this.#kvLength =
			61 +
			getDataTypeByteLength(this.#options.keyType) +
			getDataTypeByteLength(this.#options.valueType);
		this.#wal = new WalFile(this.#options.walConfig);
		

		this.#inMem.on(InMemBufferEvent.NeedsFlush, async () => {
			await this.#flush()
			await this.#wal.truncate();
		});
	}

	async #flush() {
		const data = this.#inMem.flush();
		await this.#sstManager.flushToDisk(data);
	}

	async #syncLogs() {
		const logs = await this.#wal.read(true);
		for (const log of logs) {
			this.#inMem.insert(log.data);
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
		return await this.#wal.read();
	}

	insert(key: PossibleKeyType, value: unknown) {
		const keybuffer = ValueToTypedArray(key, this.#options.keyType);
		const valueBuffer = ValueToTypedArray(value, this.#options.valueType);
		const dataBuffer = new Uint8Array(
			keybuffer.length + valueBuffer.length
		);
		let offset = 0;
		if (
			keybuffer instanceof BigInt64Array ||
			keybuffer instanceof BigUint64Array
		) {
			dataBuffer.set(new Uint8Array(keybuffer.buffer), 0);
			offset += keybuffer.length;
		} else {
			dataBuffer.set(keybuffer, 0);
			offset += keybuffer.length;
		}

		if (
			valueBuffer instanceof BigInt64Array ||
			valueBuffer instanceof BigUint64Array
		) {
			dataBuffer.set(new Uint8Array(valueBuffer.buffer), offset);
			offset += valueBuffer.length;
		} else {
			dataBuffer.set(valueBuffer, offset);
			offset += valueBuffer.length;
		}

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
			timestamp: BigInt(Date.now()),
		});

		this.#inMem.insert(node);
		this.#wal.append(node, WalMethod.Insert);
	}

	async get(key: PossibleKeyType) {
		// check in memory
		const inMemData = this.#inMem.get(key);
		if (inMemData) {
			return inMemData;
		}
		const data = await this.#sstManager.get(key);
		return data;
	}

	mayHasKey(key: PossibleKeyType) {
		return (
			this.#inMem.has(key) || (this.#sstManager.mayHasKey(key) ?? false)
		);
	}

	async has(key: PossibleKeyType) {
		return (
			this.#inMem.has(key) ||
			((await this.#sstManager.hasKey(key)) ?? false)
		);
	}

	async delete(key: PossibleKeyType) {
		// check key exists
		if (!(this.mayHasKey(key))) {
			return false;
		}
		// this part can be skipped cause we are just merging the data in sst later on
		// const node = await this.get(key);
		// if (node === null) {
		// 	return false;
		// }

		const newNode = DataNode.deletedNode(key, this.#options.keyType, this.#options.valueType);

		this.#inMem.insert(newNode);
		this.#wal.append(newNode, WalMethod.Delete);
	}

	async close() {
		await this.#flush();
		await this.#sstManager.close();
		await this.#wal.close();
	}

	async clear() {
		await this.#sstManager.clear();
		await this.#wal.truncate();
		this.#inMem.clear();
	}
}
