import xxhash from "xxhash-wasm";
import {BloomFilter} from '../../dist/structs/Filters.js';
import {getCellAndHashCount} from '../../dist/utils/bloom.js';
import { get4pointRangeOfDataType } from "../../dist/utils/dataType.js";
const { h32, h64 } = await xxhash();
import b from 'benny';
const range = get4pointRangeOfDataType("u32");
const last = range[3];
import * as customHashes from '../../dist/misc/customHash.js';
//discord id
function getRandom() {
	let res = '';
	for (let i = 0; i < 18; i++) {
		res += Math.floor(Math.random() * 10);
	}
	return res;
}
import m from "mnemonist";
const MBF = m.BloomFilter;
class OldBloomFilter {
	#bits
	#hashCount

	constructor(bitSize, hashCount) {
		this.#bits = new Array(bitSize).fill(0);
		this.#hashCount = hashCount;
	}

	add(key) {
		for (let i = 0; i < this.#hashCount; i++) {
			const index = h32(key, i) % this.#bits.length;
			this.#bits[index] = 1;
		}
	}

	lookup(key) {
		for (let i = 0; i < this.#hashCount; i++) {
			const index = h32(key, i) % this.#bits.length;
			if (!this.#bits[index]) {
				return false;
			}
		}
		return false;
	}
	setBits(bits) {
		this.#bits = bits;
	}

	setHashCount(hashCount) {
		this.#hashCount = hashCount;
	}
	
	clear() {
		this.#bits.fill(0);
	}

	get bits() {
		return this.#bits;
	}

	get hashCount() {
		return this.#hashCount;
	}
}

const [bitCount, hashCount] = getCellAndHashCount(10000,0.2);

const newbloom = new BloomFilter(bitCount, hashCount,'str:18', customHashes.hashDiscordIdCharWise);
const oldbloom = new OldBloomFilter(bitCount, hashCount);
const mbf = new MBF({
	capacity: 10000,
	errorRate: 0.2,
});

await b.suite(
	'Old vs New Bloom Filter',
	b.add('Old Bloom Filter', () => {
		oldbloom.add(getRandom());
	}),
	b.add('New Bloom Filter', () => {
		newbloom.add(getRandom());
	}),
	b.add('Mnemonist Bloom Filter', () => {
		mbf.add(getRandom());
	}),
	b.add('Old Bloom Filter Lookup', () => {
		oldbloom.lookup(getRandom());
	}),
	b.add('New Bloom Filter Lookup', () => {
		newbloom.lookup(getRandom());
	}),
	b.add('Mnemonist Bloom Filter Lookup', () => {
		mbf.test(getRandom());
	}),
	b.cycle(),
	b.complete(),
);
console.log(getRandom())

console.log(oldbloom.bits.length, newbloom.bits.length, mbf.toJSON().length);
console.log(oldbloom.hashCount, newbloom.hashCount, mbf.hashFunctions);