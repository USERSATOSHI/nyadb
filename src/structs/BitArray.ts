export default class BitArray {
	#bits: Uint8Array;
	#size: number;
	constructor(size: number, bits?:number[]) {
		this.#bits = new Uint8Array(Math.ceil(size / 8));
		this.#size = Math.ceil(size / 8);

		if(bits){
			this.#bits = new Uint8Array(bits);
		}
	}

	set(index: number) {
		this.#bits[index >> 3] |= 1 << (index & 7);
	}

	unset(index: number) {
		this.#bits[index >> 3] &= ~(1 << (index & 7));
	}

	get(index: number) {
		return (this.#bits[index >> 3] & (1 << (index & 7))) !== 0;
	}

	toArray() {
		return this.#bits;
	}

	setArray(bits: Uint8Array) {
		this.#bits = bits;
	}

	clear() {
		this.#bits.fill(0);
	}

	get length() {
		return this.#size*8;
	}

	get size() {
		return this.#size;
	}

	toString() {
		return this.#bits.toString();
	}

	//merge the bitArray with current this.#bits , bitArray is always smaller than this.#bits
	merge(bitArray: BitArray) {
		const bits = bitArray.toArray();
		for (let i = 0; i < bits.length; i++) {
			this.#bits[i] |= bits[i];
		}
	}
}