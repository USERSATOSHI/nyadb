import { hashInt, hashu64 } from "../../dist/utils/hash.js";
import b from "benny";
import xxhash from "xxhash-wasm";
const { h32, h64, h32Raw } = await xxhash();
import murmurhash from "murmurhash";
import { murmurhash3 } from "../../dist/misc/customHash.js";

function hashBigIntToU32WithSeed(bigint, seed) {
	seed = seed >>> 0;

	const low32 = Number(bigint & 0xffffffffn);
	const high32 = Number((bigint >> 32n) & 0xffffffffn);

	const unsignedLow32 = low32 >>> 0;
	const unsignedHigh32 = high32 >>> 0;

	let hash = seed;

	hash ^= unsignedLow32;
	hash = (hash ^ (hash >>> 16)) * 0x85ebca6b;
	hash ^= unsignedHigh32;
	hash = (hash ^ (hash >>> 13)) * 0xc2b2ae35;
	hash ^= hash >>> 16;

	return hash >>> 0;
}

function hashBigIntToU32XorFoldWithSeed(bigint, seed) {
	seed = seed >>> 0;

	let hash = seed;

	while (bigint > 0n) {
		hash ^= Number(bigint & 0xffffffffn);
		bigint >>= 32n;
	}

	hash ^= hash >>> 16;
	hash *= 0x85ebca6b;
	hash ^= hash >>> 13;
	hash *= 0xc2b2ae35;
	hash ^= hash >>> 16;

	return hash >>> 0;
}

function Hash(str) {
	let res = 0;
	for (let i = 0; i < str.length; i++) {
		res += (i * str[i].charCodeAt(0)) % Number.MAX_SAFE_INTEGER;
	}
	return res;
}
const num = 1234567890;
const bigNum = BigInt(num);

// convert num to uint8array
const uint8array = new Uint8Array(4);
const view = new DataView(uint8array.buffer);
view.setUint32(0, num, true);

await b.suite(
	"hashInt vs hashu64 vs h32 vs h64",
	b.add("hashInt", () => {
		hashInt(num, 0xcafe);
	}),
	b.add("hashu64", () => {
		hashu64(bigNum, 0xcafe);
	}),
	b.add("newh64", () => {
		hashBigIntToU32WithSeed(bigNum, 0xcafe);
	}),
	b.add("newh64XorFold", () => {
		hashBigIntToU32XorFoldWithSeed(bigNum, 0xcafe);
	}),
	b.add("h32", () => {
		h32(num + "", 0xcafe);
	}),
	b.add("h64", () => {
		Number(h64(num + "", 0xcafen));
	}),
	b.add("h32Raw", () => {
		h32Raw(uint8array, 0xcafe);
	}),
	b.add("Hash", () => {
		Hash(num + "");
	}),
	b.add("murmurhashv2", () => {
		murmurhash.v2(num + "", 0xcafe);
	}),
	b.add("murmurhashv3", () => {
		murmurhash.v3(num + "", 0xcafe);
	}),
	b.add("murmurhashv3custom", () => {
		murmurhash.v3(num + "", 0xcafe);
	}),
	b.cycle(),
	b.complete(),
	b.save({ file: "hashbench", version: "1.0.0" })
);
