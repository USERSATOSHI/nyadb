import { u32, u64, i64, signedInt, unsignedInt } from "../typings/type.js";

export function hashInt(num: signedInt | unsignedInt, seed: number = 0): u32 {
	// seed the num
	num = num ^ seed;
	num = num + (seed << 13);

	num = ((num >> 16) ^ num) * 0x45d9f3b;
	num = ((num >> 16) ^ num) * 0x45d9f3b;
	num = (num >> 16) ^ num;
	return num;
}

export function hashu64(num: u64 | i64, seed: number = 0): u32 {
	seed = seed >>> 0;

	const low32 = Number(num & 0xffffffffn);
	const high32 = Number((num >> 32n) & 0xffffffffn);

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
