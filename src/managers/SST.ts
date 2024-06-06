// @ts-nocheck
import fsp, { FileHandle } from "node:fs/promises";
import fs from "node:fs";
import { Worker } from "node:worker_threads";

import SSTFile from "../files/SST.js";
import { ISSTMangerOptions } from "../typings/interface.js";
import {
	get4pointRangeOfDataType,
	getDataTypeByteLength,
} from "../utils/dataType.js";
import DataNode from "../structs/Node.js";
import Mutex from "../structs/Mutex.js";
import { PossibleKeyType } from "../typings/type.js";
import path from "node:path";

export default class SSTManager {
	#options: ISSTMangerOptions;
	#mutex: Mutex = new Mutex();
	#thresoldForMerge: number;
	#sstables: SSTFile[] = [];
	#worker: Worker | null = null;
	#dirHandle: FileHandle | null = null;
	#interval: NodeJS.Timeout | null = null;

	constructor(options: ISSTMangerOptions) {
		this.#options = options;
		this.#thresoldForMerge = this.#options.growthFactor;
		this.#enableInterval();
		this.#worker = new Worker(path.resolve(import.meta.dirname, "./Merger.js"));

		this.#worker.on(
			"message",
			async (data: { data: Uint8Array[]; files: string[] }) => {
				//console.log("flushing to disk");
				await this.flushToDisk(data.data);
				const oldSstables = [...this.#sstables];

				this.#sstables = this.#sstables.filter((s) => {
					return !data.files.includes(s.options.path);
				});

				for (const file of data.files) {
					// get the sstable file and remove it
					const sst = oldSstables.find(
						(s) => s.options.path === file
					);
					if (sst) {
						await sst.unlink();
					}
				}

				await this.#dirHandle?.sync();
				this.#mutex.unlock();
			}
		);
	}

	async #enableInterval() {
		this.#interval = setInterval(async () => {
			//console.log(this.#sstables.length, this.#thresoldForMerge, this.#mutex.isLocked());
			if (this.#sstables.length >= this.#thresoldForMerge && !this.#mutex.isLocked()) {
				await this.mergeAndCompact();
			}
		}, 10000);
	
	}

	async init() {
		this.#dirHandle = await fsp.open(
			this.#options.path,
			fs.constants.O_DIRECTORY
		);
		if (!fs.existsSync(this.#options.path)) {
			await fsp.mkdir(this.#options.path);
		}

		const files = await fsp.readdir(this.#options.path);
		for (const file of files) {
			if (file.endsWith(".sst")) {
				const sst = new SSTFile({
					...this.#options.sstConfig,
					path: `${this.#options.path}/${file}`,
					dataType: this.#options.valueType,
					keyDataType: this.#options.keyType,
				});
				await sst.open();
				this.#sstables.push(sst);
			}
		}
	}

	async clear() {
		for (const sst of this.#sstables) {
			await sst.clearData();
		}
	}

	async flushToDisk(data: Uint8Array[]) {
		const sst = new SSTFile({
			...this.#options.sstConfig,
			path: `${this.#options.path}/${Date.now()}.sst`,
			dataType: this.#options.valueType,
			keyDataType: this.#options.keyType,
			kvCount: data.length,
		});
		await sst.open();
		await sst.write(data);
		this.#sstables.push(sst);
	}

	async get(key: PossibleKeyType): Promise<DataNode | null> {
		// loop in reverse order to get the latest value
		for (let i = this.#sstables.length - 1; i >= 0; i--) {
			const sst = this.#sstables[i];
			if (sst.mayHasKey(key)) {
				const val = await sst.optreadKeyMmap(key);
				if (val) {
					return val;
				}
			}
		}

		return null;
	}

	async hasKey(key: PossibleKeyType): Promise<boolean> {
		for (let i = this.#sstables.length - 1; i >= 0; i--) {
			const sst = this.#sstables[i];
			if ( await sst.hasKey(key,this.#options.readMmap)) {
				return true;
			}
		}
		return false;
	}

	mayHasKey(key: PossibleKeyType): boolean {
		for (let i = this.#sstables.length - 1; i >= 0; i--) {
			const sst = this.#sstables[i];
			if (sst.mayHasKey(key)) {
				return true;
			}
		}
		return false;
	}

	// write a worker thread to handle merge and compact with mutex lock
	async mergeAndCompact() {
		await this.#mutex.lock();
		if (!this.#worker) {
			this.#worker = new Worker(
				new URL("./Merger.js", import.meta.dirname)
			);
		}
		// get all data into a arrayBuffer and send that to worker to be merged and compacted
		//console.log("reading all data");
		const data = (
			await Promise.all(this.#sstables.map((sst) => sst.readAll()))
		).flat();

		const Uint8 = data.map((d) => d.buffer);

		this.#worker!.postMessage({
			data: Uint8,
			options: {
				keyDataType: this.#options.keyType,
				dataType: this.#options.valueType,
			},
			files: this.#sstables.map((s) => s.options.path),
		});
	}

	async close() {
		await this.#dirHandle?.close();
		await Promise.all(this.#sstables.map((s) => s.close()));
	}
}
