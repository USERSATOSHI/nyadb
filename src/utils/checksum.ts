import { createHash } from 'crypto';

export function checksum(data: string | Uint8Array): Uint8Array | Buffer {
	const hash = createHash('sha256');
	return hash.update(data).digest() as Uint8Array | Buffer;
}

export function checksumTypedArray(data: Uint8Array): Uint8Array {
	const hash = createHash('sha256');
	return hash.update(data).digest();
}
export function validateChecksum(data: string | Uint8Array, _checksum: Uint8Array): boolean {
	return checksum(data).every((byte, i) => byte === _checksum[i]);
}