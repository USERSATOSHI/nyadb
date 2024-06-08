import fsp from "node:fs/promises";
import { Readable, Transform } from "node:stream";
import { IWalFileOptions } from "../typings/interface.js";
import { WriteStream } from "node:fs";
import DataNode from "../structs/Node.js";
import { WalMethod } from "../typings/enum.js";
import Column from "../structs/Column.js";
import { getDataTypeByteLength } from "../utils/dataType.js";

const WAL_FILE_MAGIC_NUMBER = [0x57, 0x41, 0x4c, 0x46];

export const WAL_START_DELIMITER = new Uint8Array([0x01, 0x10, 0xef, 0xfe]);
export const WAL_END_DELIMITER = new Uint8Array([0xfe, 0xef, 0x10, 0x01]);
export default class WalFile {
	#options: IWalFileOptions;
	#fileHandle!: fsp.FileHandle;
	#fileSize: number = -1;
	#writer!: WriteStream;
	#readable: Readable = new Readable({
		construct() {
			this._read = () => {};
		},
	});
	#byteCounterTransform!: Transform;
	#column: Column;
	#paused: boolean = false;
	#length: number;

	constructor(options: IWalFileOptions, column: Column) {
		this.#options = options;
		this.#column = column;
		this.#length =
			27 +
			getDataTypeByteLength(this.#column.options.keyType) +
			getDataTypeByteLength(this.#column.options.valueType);
	}

	get fileSize() {
		return this.#fileSize;
	}

	async open() {
		this.#fileHandle = await fsp.open(
			this.#options.path,
			fsp.constants.O_CREAT |
				fsp.constants.O_RDWR |
				fsp.constants.O_APPEND
		);
		this.#fileSize = (await this.#fileHandle.stat()).size;
		if (this.#fileSize === 0) {
			this.#fileHandle.appendFile(
				new Uint8Array([...WAL_FILE_MAGIC_NUMBER, 0x0a]),
				{
					flush: true,
				}
			);
		}

		this.#writer = this.#fileHandle.createWriteStream({
			flush: true,
			start: this.#fileSize,
			autoClose: false,
			emitClose: false,
		});
		// update file size
		this.#byteCounterTransform = new Transform({
			transform: (chunk, encoding, callback) => {
				this.#byteCounterTransform.push(chunk);
				callback();
				this.#fileSize += chunk.byteLength;
			},
		});

		this.#writer.once("error", async (err) => {
			this.#writer.close();
			await this.open();
		});

		//update the file size
		this.#writer.on("finish", () => {
			this.#fileSize = this.#writer.bytesWritten ?? 0;
		});

		this.#readable.pipe(this.#byteCounterTransform).pipe(this.#writer);
	}

	async close() {
		await this.#fileHandle.close();
		this.#writer.close();
		this.#readable.destroy();
		this.#byteCounterTransform.end();
	}

	append(data: DataNode, method: WalMethod) {
		let writeBuffer = data.toWAL();

		const walBuffer = new Uint8Array(9 + writeBuffer.byteLength);
		let offset = 0;
		walBuffer.set(WAL_START_DELIMITER, offset);
		offset += 4;
		walBuffer.set(writeBuffer, offset);
		offset += writeBuffer.byteLength;
		walBuffer[offset++] = method;
		walBuffer.set(WAL_END_DELIMITER, offset);

		this.#readable.push(walBuffer);
	}
	async read(returnNode: true): Promise<DataNode[]>;
	async read(returnNode: false): Promise<Uint8Array[]>;
	async read(returnNode: boolean): Promise<(DataNode | Uint8Array)[]> {
		let start = 5;
		const logLine = new Uint8Array(this.#length);
		const logs: (DataNode | Uint8Array)[] = [];

		while (start < this.#fileSize) {
			await this.#fileHandle.read(
				logLine,
				0,
				this.#length,
				start
			);
			logs.push(returnNode ? DataNode.fromWAL(logLine) : logLine);
			start += this.#length;
		}
		return logs;
	}

	async truncate() {
		this.#paused = true;
		this.#readable.pause();
		this.#writer.uncork();
		await this.#fileHandle.sync();
		await this.#fileHandle.truncate(
			new Uint8Array([...WAL_FILE_MAGIC_NUMBER, 0x0a]).byteLength
		);
		this.#fileSize = (await this.#fileHandle.stat()).size;
		this.#readable.resume();
		this.#paused = false;
	}

	stats = () => {
		return {
			fileSize: this.#fileSize,
			entries: this.#fileSize / this.#length,
		};
	}
}
