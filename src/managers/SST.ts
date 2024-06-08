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
import { cpus } from "node:os";

export default class SSTManager {
	#options: ISSTMangerOptions;
	#levelsMutex: Mutex[] = [];
	#levels: SSTFile[][];
	#dirHandle: FileHandle[] = [];
	#interval: NodeJS.Timeout | null = null;

	constructor(options: ISSTMangerOptions) {
		this.#options = options;
		this.#levels = new Array(this.#options.levels).fill(null).map(() => []);
		this.#levelsMutex = new Array(this.#options.levels)
			.fill(null)
			.map(() => new Mutex());
		this.#options.threadsForMerge =
			this.#options.threadsForMerge ?? cpus().length;
		this.#enableInterval();
	}

	async #enableInterval() {
		this.#interval = setInterval(async () => {
			// check if any level has reached the threshold
			for (let i = 0; i < this.#levels.length; i++) {
				if (
					this.#levels[i].length >= this.#options.growthFactor &&
					!this.#levelsMutex[i].isLocked()
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
		await Promise.all(
			this.#levels.map((level) => level.map((s) => s.clearData()))
		);
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

	async #closeOldFiles(data: { files: string[]; level: number }) {
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
		this.#levelsMutex[data.level].unlock();
		console.log(`Level ${data.level} has been compacted & Promoted to level ${data.level + 1}: files Compacted: ${data.files.length}`);
	}

	// write a worker thread to handle merge and compact with mutex lock
	async mergeAndCompact(level: number = 0) {
		await this.#levelsMutex[level].lock();
		const files = [...this.#levels[level]];

		const worker = new Worker(
			path.resolve(import.meta.dirname, "../workers/merge.js")
		);

		worker.on("message", async (pathOfNewSSTable: string[]) => {
			for (const path of pathOfNewSSTable) {
				const sst = new SSTFile({
					...this.#options.sstConfig,
					path,
					dataType: this.#options.valueType,
					keyDataType: this.#options.keyType,
				});
				await sst.open();
				this.#levels[level + 1].push(sst);
			}
			await this.#dirHandle[level + 1].sync();
			await worker.terminate();
			await this.#closeOldFiles({ files:files.map(x => x.options.path), level });
		});

		worker.on("error", async (err) => {
			console.error(err);
			await worker.terminate();
		});

		worker.on("exit", (code) => {
			if (code !== 0) {
				console.error(`Worker stopped with exit code ${code}`);
			}
		});

		worker.on("close", () => {
			this.#levelsMutex[level].unlock();
		});

		worker.postMessage({
			filePaths: files.map((f) => f.options.path),
			level,
			dataSize: this.#levels[level][0].metaData.kvPairLength.total,
			kvCount: this.#options.sstConfig.kvCount,
			growthFactor: this.#options.growthFactor,
			pathForNextLevel: `${this.#options.path}/level-${level + 1}`,
			options: {
				keyDataType: this.#options.keyType,
				dataType: this.#options.valueType,
			},
		});
	}

	async close() {
		await Promise.all(this.#dirHandle.map(async (d) => await d.close()));
		clearInterval(this.#interval);
		await Promise.all(
			this.#levels.map((level) => level.map((s) => s.close()))
		);
	}

	async stats() {
		return await Promise.all(
			this.#levels.map(async (level, i) => {
				return {
					level: i,
					count: level.length,
					ssts: await Promise.all(
						level.map(async (s) => await s.stats())
					),
				};
			})
		);
	}

	async ping() {
		let avgPing = 0;

		let total = 0;
		
		for (const level of this.#levels) {
			for (const sst of level) {
				total += await sst.ping();
			}
		}

		avgPing = total / this.#levels.flat().length;
		return avgPing;
	}
}
