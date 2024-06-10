import { TypedEmitter } from "tiny-typed-emitter";
import { ITableOptions } from "../typings/interface.js";
import fs from "node:fs";
import { PossibleKeyType } from "../typings/type.js";
import Column from "./Column.js";
import Database from "./Database.js";
export default class Table extends TypedEmitter {
	#name: ITableOptions["name"];
	#columns: Map<string, Column> = new Map();
	#db: Database;
	constructor(options: ITableOptions, database: Database) {
		super();
		this.#name = options.name;
		for (const column of options.columns) {
			this.#columns.set(column.options.name, column);
		}
		this.#db = database;
	}

	async init() {
		const databasePath = this.#db.options.path;
		const path = databasePath + "/" + this.#name;
		if (!fs.existsSync(path)) {
			await fs.promises.mkdir(path, { recursive: true });
		}

		for (const column of this.#columns.values()) {
			column.setPath(
				`${this.#db.options.path}/${this.#name}/${column.options.name}`
			);
			await column.init();
		}

		this.emit("ready");
	}

	async close() {
		for (const column of this.#columns.values()) {
			await column.close();
		}
	}

	insert(column: string, key: PossibleKeyType, value: PossibleKeyType) {
		const col = this.#columns.get(column);
		if (!col) {
			throw new Error("Column not found");
		}
		col.insert(key, value);
	}

	async get(column: string, key: PossibleKeyType) {
		const col = this.#columns.get(column);
		if (!col) {
			throw new Error("Column not found");
		}
		return await col.get(key);
	}

	async has(column: string, key: PossibleKeyType) {
		const col = this.#columns.get(column);
		if (!col) {
			throw new Error("Column not found");
		}
		return await col.has(key);
	}

	bloomCheck(column: string, key: PossibleKeyType) {
		const col = this.#columns.get(column);
		if (!col) {
			throw new Error("Column not found");
		}
		return col.mayHasKey(key);
	}

	async delete(column: string, key: PossibleKeyType) {
		const col = this.#columns.get(column);
		if (!col) {
			throw new Error("Column not found");
		}
		await col.delete(key);
	}

	async clear(column?: string): Promise<void> {
		if (column) {
			const col = this.#columns.get(column);
			if (!col) {
				throw new Error("Column not found");
			}
			await col.clear();
		} else {
			for (const col of this.#columns.values()) {
				await col.clear();
			}
		}
	}

	async ping() {
		let avgPing = 0;
		for (const col of this.#columns.values()) {
			avgPing += await col.ping();
		}
		return avgPing / this.#columns.size;
	}

	async stats() {
		const stats = [];
		for (const col of this.#columns.values()) {
			stats.push(await col.stats());
		}
		return stats;
	}

	get name() {
		return this.#name;
	}

	get columns() {
		return this.#columns;
	}

	get database() {
		return this.#db;
	}

	hasColumn(column: string) {
		return this.#columns.has(column);
	}
}
