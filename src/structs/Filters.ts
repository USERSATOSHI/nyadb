import xxhash from "xxhash-wasm";
import BitArray from "./BitArray.js";
import { availableDataTypeForHash, dataType, HashFunction, HashInputType, numDataTypes, signedInt, str, u32, unsignedInt } from "../typings/type.js";
import { hashInt, hashu64 } from "../utils/hash.js";
import { murmurhash3 } from "../misc/customHash.js";

const { h32Raw } = await xxhash();

export class BloomFilter<T extends availableDataTypeForHash> {
	#bits: BitArray;
	#hashCount: number;
	#bitArray?: Uint8Array;
	#hashFunction: HashFunction<T>;
	constructor(bitSize:number, hashCount: number, dataType: T, customHashFunction?: HashFunction<T> | null) {
		this.#bits = new BitArray(bitSize);
		this.#hashCount = hashCount;
		if (dataType.startsWith("str:")) {
			this.#bitArray = new Uint8Array(Number(dataType.split(":")[1]));
			this.#hashFunction = murmurhash3 as HashFunction<T>;
		} else if (dataType === "i64" || dataType === "u64") {
			this.#hashFunction = hashu64 as HashFunction<T>;
		} else {
			this.#hashFunction = hashInt as HashFunction<T>;
		}

		if (customHashFunction) {
			this.#hashFunction = customHashFunction;
		}
	}

	#toUint8Array = (input: str): Uint8Array => {
		for (let i = 0; i < input.length; i++) {
			this.#bitArray![i] = input.charCodeAt(i);
		}
		return this.#bitArray!;
	}
	
	add(key:HashInputType<T> ) {
		let keyToHash: Uint8Array | HashInputType<T> = key;
		if (this.#bitArray) {
			keyToHash = this.#toUint8Array(key as str);
		}
		for (let i = 0, l = this.#hashCount; i < l; i++) {
			// @ts-ignore
			const index = this.#hashFunction(keyToHash, (i * 0xFBA4C795) & 0xFFFFFFFF) % this.#bits.length;
			this.#bits.set(index);
		}
	}

	lookup(key:HashInputType<T>) {
		let keyToHash: Uint8Array | HashInputType<T> = key;
		if (this.#bitArray) {
			keyToHash = this.#toUint8Array(key as str);
		}
		for (let i = 0,l = this.#hashCount; i < l; i++) {
			// @ts-ignore
			const index = this.#hashFunction(keyToHash, (i * 0xFBA4C795) & 0xFFFFFFFF) % this.#bits.length;
			if (!this.#bits.get(index)) return false;
		}
		return true;
	}
	setBits(bits: number[]) {
		this.#bits = new BitArray(bits.length, bits);
	}

	setHashCount(hashCount: number) {
		this.#hashCount = hashCount;
	}
	
	clear() {
		this.#bits.clear();
	}

	get bits() {
		return this.#bits;
	}
	get hashCount() {
		return this.#hashCount;
	}
}