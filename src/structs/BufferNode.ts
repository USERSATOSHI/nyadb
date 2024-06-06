import { dataType } from "../typings/type.js";
import DataNode from "./Node.js";

export default class BufferNode {
	#data: Uint8Array;
	#offset: number = 0;
	#keyType: dataType;
	#valueType: dataType;

	constructor(
		data: Uint8Array,
		offset: number = 0,
		keyType: dataType,
		valueType: dataType
	) {
		this.#data = data;
		this.#offset = offset;
		this.#keyType = keyType;
		this.#valueType = valueType;
	}

	build() {
		return DataNode.fromUint8Array(this.#data, this.#offset, {keyDataType: this.#keyType, dataType: this.#valueType});
	}

	get buffer() {
		return this.#data;
	}
}
