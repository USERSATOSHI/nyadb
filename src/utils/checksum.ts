import xxhash from "xxhash-wasm";
const { h32Raw } = await xxhash();

export function checksum(data: Uint8Array): Uint8Array | Buffer {
	const hash = h32Raw(data, 0xcafebabe);
	const u8 = new Uint8Array(4);
	u8[0] = hash & 0xff;
	u8[1] = (hash >> 8) & 0xff;
	u8[2] = (hash >> 16) & 0xff;
	u8[3] = (hash >> 24) & 0xff;
	return u8;
}

export function validateChecksum(
	data: Uint8Array,
	_checksum: Uint8Array
): boolean {
	return checksum(data).every((byte, i) => byte === _checksum[i]);
}
