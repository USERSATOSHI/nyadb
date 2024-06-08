import { TypedEmitter } from "tiny-typed-emitter";
import { IDatabaseOptions } from "../typings/interface.js";
import fs from "node:fs";
import Table from "./Table.js";
import { PossibleKeyType } from "../typings/type.js";
export default class Database extends TypedEmitter {
	#options: IDatabaseOptions;
	#tables: Map<string, Table> = new Map();
	#readyAt: number = -1;
	constructor(options: IDatabaseOptions) {
		super();
		this.#options = options;
	}

	#areAllTbalesReady(table: Table) {
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

		for (const table of this.#options.tables) {
			const t = new Table(table, this);
			t.on("ready", () => this.#areAllTbalesReady(t));
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

	async bloomCheck(
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
		return await tbl.bloomCheck(column, key);
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
