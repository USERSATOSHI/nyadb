import EventEmitter from "events";
import { IInMemoryBufferOptions } from "../typings/interface.js";
import { PossibleKeyType } from "../typings/type.js";
import DataNode from "./Node.js";
import { OrderedMap, type OrderedMapIterator } from "@js-sdsl/ordered-map";
import { InMemBufferEvents } from "../typings/enum.js";
import WalFile from "../files/Wal.js";

export default class InMemoryBuffer extends EventEmitter {

	#options: IInMemoryBufferOptions;
	#buffer: OrderedMap<DataNode["key"], DataNode>;
	#lock: boolean = false;
	#iter: OrderedMapIterator<DataNode["key"], DataNode>;
	#waitQueue: [DataNode["key"], DataNode][] = []; 

	constructor(options: IInMemoryBufferOptions) {
		super();
		this.#options = options;
		this.#buffer = new OrderedMap();
		this.#iter = this.#buffer.begin();
	}

	get size() {
		return this.#buffer.size();
	}

	get isEmpty() {
		return this.#buffer.empty();
	}

	get options() {
		return this.#options;
	}

	isLocked() {
		return this.#lock;
	}

	insert(data: DataNode) {
		if (this.#lock) {
			this.#waitQueue.push([data.key, data]);
			return;
		}
		this.#buffer.setElement(data.key, data, this.#iter);
		if (this.size >= this.#options.threshHold) {
			this.#lock = true;
			this.emit(InMemBufferEvents.NeedsFlush);
		}
	}

	has(key: PossibleKeyType) {
		return this.#buffer.find(key) !== this.#buffer.end();
	}

	get(key: PossibleKeyType) {
		return this.#buffer.getElementByKey(key);
	}

	flush() {
		this.#lock = false;
		const buffer = this.#buffer;
		this.#buffer = new OrderedMap(this.#waitQueue);
		this.#waitQueue = [];
		this.#iter = this.#buffer.begin();
		this.emit(InMemBufferEvents.BufferOpened);
		const data = [];
		for (const [_, value] of buffer) {
			data.push(value.toUint8Array());
		}
		return data;
	}

	clear() {
		this.#buffer.clear();
		this.#iter = this.#buffer.begin();
		this.#lock = false;
	}
}
