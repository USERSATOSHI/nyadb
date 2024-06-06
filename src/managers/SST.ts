// @ts-nocheck
import fsp, { FileHandle } from "node:fs/promises";
import fs from "node:fs";
import { Worker } from "node:worker_threads";

import SSTFile from "../files/SST.js";
import { ISSTMangerOptions } from "../typings/interface.js";
import DataNode from "../structs/Node.js";
import Mutex from "../structs/Mutex.js";
import { PossibleKeyType } from "../typings/type.js";
import path from "node:path";
import BufferNode from "../structs/BufferNode.js";
import { chunkify, sortAndMerge } from "../utils/sortAndMerge.js";
import { cpus } from "node:os";

export default class SSTManager {
	#options: ISSTMangerOptions;
	#mutex: Mutex = new Mutex();
	#thresoldForMerge: number;
	#levels: SSTFile[][];
	#dirHandle: FileHandle[] = [];
	#interval: NodeJS.Timeout | null = null;

	constructor(options: ISSTMangerOptions) {
		this.#options = options;
		this.#thresoldForMerge = this.#options.growthFactor;
		this.#levels = new Array(this.#options.levels).fill(null).map(() => []);
		this.#options.threadsForMerge = this.#options.threadsForMerge ?? cpus().length;
		this.#enableInterval();
	}

	async #enableInterval() {
		this.#interval = setInterval(async () => {
			// check if any level has reached the threshold
			for (let i = 0; i < this.#levels.length; i++) {
				if (
					this.#levels[i].length >= this.#thresoldForMerge &&
					!this.#mutex.isLocked()
				) {
					await this.mergeAndCompact(i);
				}
			}
		}, 10000);
	}

	async init() {
		if (!fs.existsSync(this.#options.path)) {
			await fsp.mkdir(this.#options.path);
		}

		// check we have levels from 0 to n
		for (let i = 0; i < this.#options.levels; i++) {
			if (!fs.existsSync(`${this.#options.path}/level-${i}`)) {
				await fsp.mkdir(`${this.#options.path}/level-${i}`);
			}
		}

		const files = await fsp.readdir(this.#options.path);
		for (const file of files) {
			const stat = await fsp.stat(`${this.#options.path}/${file}`);
			if (stat.isDirectory()) {
				this.#dirHandle.push(
					await fsp.open(`${this.#options.path}/${file}`)
				);
				// get ss tables from each level
				const sstFiles = await fsp.readdir(
					`${this.#options.path}/${file}`
				);

				for (const sstFile of sstFiles) {
					if (sstFile.endsWith(".sst")) {
						const sst = new SSTFile({
							...this.#options.sstConfig,
							path: `${this.#options.path}/${file}/${sstFile}`,
							dataType: this.#options.valueType,
							keyDataType: this.#options.keyType,
						});
						await sst.open();
						this.#levels[parseInt(file.split("-")[1])].push(sst);
					}
				}
			}
		}
	}

	async clear() {
		await Promise.all(this.#levels.map((level) => level.map((s) => s.clearData())));
	}

	async flushToDisk(data: Uint8Array[], level: number = 0) {
		const sst = new SSTFile({
			...this.#options.sstConfig,
			path: `${this.#options.path}/level-${level}/${Date.now()}.sst`,
			dataType: this.#options.valueType,
			keyDataType: this.#options.keyType,
			kvCount: data.length,
		});
		await sst.open();
		await sst.write(data);
		this.#levels[level].push(sst);
	}

	async get(key: PossibleKeyType): Promise<DataNode | null> {
		// loop in reverse order to get the latest value
		for (let j = 0; j < this.#levels.length; j++) {
			for (let i = this.#levels[j].length - 1; i >= 0; i--) {
				const sst = this.#levels[j][i];
				if (sst.mayHasKey(key)) {
					const val = await sst.optreadKeyMmap(key);
					if (val) {
						return val;
					}
				}
			}
		}

		return null;
	}

	async hasKey(key: PossibleKeyType): Promise<boolean> {
		for (let j = 0; j < this.#levels.length; j++) {
			for (let i = this.#levels[j].length - 1; i >= 0; i--) {
				const sst = this.#levels[j][i];
				if (await sst.hasKey(key)) {
					return true;
				}
			}
		}

		return false;
	}

	mayHasKey(key: PossibleKeyType): boolean {
		for (let j = 0; j < this.#levels.length; j++) {
			for (let i = this.#levels[j].length - 1; i >= 0; i--) {
				const sst = this.#levels[j][i];
				if (sst.mayHasKey(key)) {
					return true;
				}
			}
		}

		return false;
	}

	async #mergeAndCompact(data: {
		data: Uint8Array[];
		files: string[];
		level: number;
	}) {
		//console.log("flushing to disk");
		await this.flushToDisk(data.data, Math.min(data.level + 1, this.#options.levels - 1));
		const oldSstables = [...this.#levels[data.level]];

		this.#levels[data.level] = this.#levels[data.level].filter(
			(s) => !data.files.includes(s.options.path)
		);

		for (const file of data.files) {
			// get the sstable file and remove it
			const sst = oldSstables.find((s) => s.options.path === file);
			if (sst) {
				await sst.unlink();
			}
		}

		await this.#dirHandle[data.level].sync();
		this.#mutex.unlock();
	}

	// write a worker thread to handle merge and compact with mutex lock
	async mergeAndCompact(level: number = 0) {
		await this.#mutex.lock();

		const files = [...this.#levels[level]];
		const data: DataNode["data"][][] = [];
		for (const file of files) {
			const data_: DataNode["data"][] = await file.readAll(true);
			data.push(data_);
		}

		const chunks = chunkify(data, this.#options.threadsForMerge);
		const partialSorted = [];
		for (const chunk of chunks) {
			const worker = new Worker(
				path.resolve(import.meta.dirname, "./Merger.js")
			);
			worker.postMessage({
				data: chunk,
				options: {
					keyDataType: this.#options.keyType,
					dataType: this.#options.valueType,
				},
				files: files.map((s) => s.options.path),
				level,
			});

			worker.on("message", async (data) => {
				partialSorted.push(data.data);
				if (partialSorted.length === chunks.length) {
					await worker.terminate();
					// join all the partial sorted data
					const mergedData = sortAndMerge(partialSorted, true);
					await this.#mergeAndCompact({
						data: mergedData,
						files: files.map((s) => s.options.path),
						level,
					});
				}
			});
		}
	}

	async close() {
		await this.#dirHandle?.close();
		clearInterval(this.#interval);
		await Promise.all(this.#levels.map((level) => level.map((s) => s.close())));
	}
}
