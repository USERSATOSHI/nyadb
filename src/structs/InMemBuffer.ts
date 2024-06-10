import { IInMemoryBufferOptions, IInMemBufferEvents } from "../typings/interface.js";
import { PossibleKeyType } from "../typings/type.js";
import DataNode from "./Node.js";
import { OrderedMap, type OrderedMapIterator } from "@js-sdsl/ordered-map";
import { InMemBufferEvent } from "../typings/enum.js";
import WalFile from "../files/Wal.js";
import { TypedEmitter } from "tiny-typed-emitter";
import { mergeOptions } from "../utils/mergeOptions.js";

export default class InMemoryBuffer extends TypedEmitter<IInMemBufferEvents> {

	#options: IInMemoryBufferOptions;
	#buffer: OrderedMap<DataNode["key"], DataNode>;
	#lock: boolean = false;
	#iter: OrderedMapIterator<DataNode["key"], DataNode>;
	#waitQueue: [DataNode["key"], DataNode][] = []; 

	static defaultOptions(): IInMemoryBufferOptions {
		return {
			threshHold: 100000,
		};
	}

	constructor(options: IInMemoryBufferOptions) {
		super();
		this.#options = mergeOptions(InMemoryBuffer.defaultOptions(), options);
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
			this.emit(InMemBufferEvent.NeedsFlush);
		}
	}

	has(key: PossibleKeyType) {
		return  this.#buffer.find(key) !== this.#buffer.end() || this.#waitQueue.find(([k, _]) => k === key) !== undefined;
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
		this.emit(InMemBufferEvent.BufferOpened);
		const data = new Array<Uint8Array>(buffer.size());

		let idx = 0;
		for (const [_, node] of buffer) {
			data[idx++] = node.toUint8Array();
		}

		return data;
	}

	clear() {
		this.#buffer.clear();
		this.#iter = this.#buffer.begin();
		this.#lock = false;
	}

	stats() {
		return {
			size: this.size,
			isEmpty: this.isEmpty,
			lock: this.#lock,
		};
	}
}