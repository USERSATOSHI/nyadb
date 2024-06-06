import { FileDataType } from "./enum.js";

export type dataType = `str:${number}` | 'i8' | 'i16' | 'i32' | 'i64' | 'u8' | 'u16' | 'u32' | 'u64' | 'f32' | 'f64' | 'bool';

export type DeepRequired<T> = {
	[P in keyof T]-?: DeepRequired<T[P]>;
};

export type TypedArray = Uint8Array | Uint16Array | Uint32Array | BigUint64Array | Int8Array | Int16Array | Int32Array | BigInt64Array | Float32Array | Float64Array;

export type FileDataTypeMember = keyof typeof  FileDataType;

export type PossibleKeyType = string | number | bigint | boolean;