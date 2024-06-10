import { TypedEmitter } from "tiny-typed-emitter";
import { IDatabaseOptions } from "../typings/interface.js";
import fs from "node:fs";
import Table from "./Table.js";
import { PossibleKeyType } from "../typings/type.js";
import * as tar from "tar";
export default class Database extends TypedEmitter {
	#options: IDatabaseOptions;
	#tables: Map<string, Table> = new Map();
	#readyAt: number = -1;
	constructor(options: IDatabaseOptions) {
		super();
		this.#options = options;
	}

	#areAllTablesReady(table: Table) {
		if (this.#tables.size === this.#options.tables.length) {
			this.emit("ready");
			this.#readyAt = Date.now();
		} else {
			this.#tables.set(table.name, table);
		}
	}

	async init() {
		const path = this.#options.path;
		if (!fs.existsSync(path)) {
			await fs.promises.mkdir(path, { recursive: true });
		}

		if( !fs.existsSync(path +'/.snapshots')){
			await fs.promises.mkdir(path +'/.snapshots', { recursive: true });
		}

		for (const table of this.#options.tables) {
			const t = new Table(table, this);
			t.on("ready", () => this.#areAllTablesReady(t));
			await t.init();
		}

		return this;
	}

	async close() {
		for (const table of this.#tables.values()) {
			await table.close();
		}
	}

	insert(
		table: string,
		data: {
			column: string;
			key: PossibleKeyType;
			value: PossibleKeyType;
		}
	) {
		const { column, key, value } = data;
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}
		tbl.insert(column, key, value);
	}

	async get(
		table: string,
		data: {
			column: string;
			key: PossibleKeyType;
		}
	) {
		const { column, key } = data;
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}
		return await tbl.get(column, key);
	}

	async has(
		table: string,
		data: {
			column: string;
			key: PossibleKeyType;
		}
	) {
		const { column, key } = data;
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}
		return await tbl.has(column, key);
	}

	bloomCheck(
		table: string,
		data: {
			column: string;
			key: PossibleKeyType;
		}
	) {
		const { column, key } = data;
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}
		return tbl.bloomCheck(column, key);
	}

	async delete(
		table: string,
		data: {
			column: string;
			key: PossibleKeyType;
		}
	) {
		const { column, key } = data;
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}
		await tbl.delete(column, key);
	}

	async clear(table: string, column?: string) {
		if (!this.#tables.size) {
			throw new Error("Database not ready");
		}
		const tbl = this.#tables.get(table);
		if (!tbl) {
			throw new Error("Table not found");
		}

		await tbl.clear(column);
	}

	async ping() {
		let avgPing = 0;
		for (const tbl of this.#tables.values()) {
			avgPing += await tbl.ping();
		}
		return avgPing / this.#tables.size;
	}

	async stats() {
		const stats = [];
		for (const tbl of this.#tables.values()) {
			stats.push(await tbl.stats());
		}
		return stats;
	}

	async snapshot() {
		// take snapshot of database and store it in .snapshots
		const path = this.#options.path;
		const snapshotPath = path + '/.snapshots/' + Date.now() + '.tar';
		// excllude .snapshots folder
		tar.c({
			file: snapshotPath,
			cwd: path,
			gzip: true,
			// exclude .snapshots folder
			filter(path, entry) {
				return !path.includes('.snapshots');
			},
		}, ['.']);
		return snapshotPath;

	}

	async restoreFromSnapshot(snapshotPath: string) {
		// restore database from snapshot
		const path = this.#options.path;
		await tar.x({
			file: snapshotPath,
			cwd: path,
			keep: false,
			unlink: true,
		});
	}

	get readyAt() {
		return this.#readyAt;
	}

	get tables() {
		return this.#tables;
	}

	get options() {
		return this.#options;
	}

	get path() {
		return this.#options.path;
	}

	get name() {
		return this.#options.name;
	}

	get size() {
		return this.#tables.size;
	}
}
