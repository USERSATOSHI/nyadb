import fsp from "node:fs/promises";

import { IWalFileOptions } from "../typings/interface.js";
import { WriteStream } from "node:fs";
import DataNode from "../structs/Node.js";
import { WalMethod } from "../typings/enum.js";

const WAL_FILE_MAGIC_NUMBER = [0x57, 0x41, 0x4c, 0x46];

export const WAL_DELIMITER = ";;;(-.-);;;";

const WAL_END_DELIMITER =
	"---;;;;----;;;;----;;;;----(o.o)----;;;;----;;;;----;;;;---";

export default class WalFile {
	#options: IWalFileOptions;
	#fileHandle!: fsp.FileHandle;
	#fileSize: number = -1;
	#writer!: WriteStream;

	constructor(options: IWalFileOptions) {
		this.#options = options;
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
	}

	async close() {
		await this.#fileHandle.close();
	}

	append(data: DataNode, method: WalMethod) {
		let writeString = data.toWAL();
		writeString += `${WAL_DELIMITER}${method}${WAL_END_DELIMITER}`;
		this.#writer.write(writeString);
		this.#fileSize += writeString.length;
	}

	async read<T extends boolean>(
		// @ts-ignore - Ik what I'm doing
		returnNode: T = false
	): Promise<
		Awaited<
			T extends true
				? { method: WalMethod; data: DataNode }[]
				: { method: WalMethod; data: Uint8Array }[]
		>
	> {
		const logsString = (await this.#fileHandle.readFile())
			.slice(5)
			.toString();
		const logs = logsString.split(WAL_END_DELIMITER);
		logs.pop();

		const logsData = logs.map((log) => {
			const data = log.split(WAL_DELIMITER);
			const method = data.pop();
			const node = DataNode.fromWAL(data.join(WAL_DELIMITER));
			return {
				data: returnNode ? node : node.toUint8Array(),
				method: Number(method) as WalMethod,
			};
		}) as Awaited<
			T extends true
				? { method: WalMethod; data: DataNode }[]
				: { method: WalMethod; data: Uint8Array }[]
		>;

		return logsData;
	}

	async truncate() {
		await this.#fileHandle.truncate(
			new Uint8Array([...WAL_FILE_MAGIC_NUMBER, 0x0a]).byteLength
		);
	}
}
