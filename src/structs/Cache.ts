import mnemonist from "mnemonist";
import DataNode from "./Node.js";
const { LRUMapWithDelete } = mnemonist;
// import { LRUCache } from "lru-cache";

export default class Cache {
	#cache: mnemonist.LRUMapWithDelete<DataNode["key"], DataNode>;
	#capacity: number;

	constructor(capacity: number) {
		this.#cache = new LRUMapWithDelete(capacity);
		this.#capacity = capacity;
	}

	set(key: DataNode["key"], value: DataNode) {
		this.#cache.set(key, value);
	}

	get(key: DataNode["key"]) {
		return this.#cache.get(key);
	}

	get capacity() {
		return this.#capacity;
	}

	get size() {
		return this.#cache.size;
	}

	clear() {
		this.#cache.clear();
	}

	[Symbol.iterator]() {
		return this.#cache.entries();
	}

	entries() {
		return this.#cache.entries();
	}

	keys() {
		return this.#cache.keys();
	}

	values() {
		return this.#cache.values();
	}

	has(key: DataNode["key"]) {
		return this.#cache.has(key);
	}

	delete(key: DataNode["key"]) {
		return this.#cache.delete(key);
	}
	find(
		predicate: (
			value: DataNode,
			key: DataNode["key"],
			cache: Cache
		) => boolean
	) {
		for (let [key, value] of this.#cache) {
			if (predicate(value, key, this)) {
				return value;
			}
		}
	}

	map(
		predicate: (value: DataNode, key: DataNode["key"], cache: Cache) => any
	) {
		const result = [];
		for (let [key, value] of this.#cache) {
			result.push(predicate(value, key, this));
		}
		return result;
	}
}
