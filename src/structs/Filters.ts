import murmurhash from "murmurhash";

export class BloomFilter {
	#bits: number[];
	#hashCount: number;

	constructor(bitSize:number, hashCount: number) {
		this.#bits = new Array(bitSize).fill(0);
		this.#hashCount = hashCount;
	}

	add(key: string) {
		for (let i = 0; i < this.#hashCount; i++) {
			const index = murmurhash.v3(key, i) % this.#bits.length;
			this.#bits[index] = 1;
		}
	}

	lookup(key:string) {
		for (let i = 0; i < this.#hashCount; i++) {
			const index = murmurhash.v3(key, i) % this.#bits.length;
			if(!this.#bits[index]) return false;
		}
		return false;
	}
	setBits(bits: number[]) {
		this.#bits = bits;
	}

	setHashCount(hashCount: number) {
		this.#hashCount = hashCount;
	}
	
	clear() {
		this.#bits.fill(0);
	}

	get bits() {
		return this.#bits;
	}
}