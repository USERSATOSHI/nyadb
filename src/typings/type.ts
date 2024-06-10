import { FileDataType } from "./enum.js";

export type dataType =
	| `str:${number}`
	| "i8"
	| "i16"
	| "i32"
	| "i64"
	| "u8"
	| "u16"
	| "u32"
	| "u64"
	| "f32"
	| "f64"
	| "bool";

export type DeepRequired<T> = {
	[P in keyof T]-?: DeepRequired<T[P]>;
};

export type TypedArray =
	| Uint8Array
	| Uint16Array
	| Uint32Array
	| BigUint64Array
	| Int8Array
	| Int16Array
	| Int32Array
	| BigInt64Array
	| Float32Array
	| Float64Array;

export type FileDataTypeMember = keyof typeof FileDataType;

export type PossibleKeyType = string | number | bigint;

export type u32 = number;
export type u64 = bigint;
export type i32 = number;
export type i64 = bigint;
export type f32 = number;
export type f64 = number;
export type str = string;
export type bool = boolean;
export type u8 = number;
export type i8 = number;
export type u16 = number;
export type i16 = number;

export type signedInt = i8 | i16 | i32;
export type float = f32 | f64;
export type unsignedInt = u8 | u16 | u32;

export type HashFunction<T extends dataType> = T extends `$str:${number}`
	? (key: Uint8Array, seed: number) => u32
	: T extends "i64" | "u64"
	? (key: bigint, seed: number) => u32
	: (key: signedInt | unsignedInt, seed: number) => u32;

export type HashInputType<T extends dataType> = T extends `str:${number}`
	? string
	: T extends "i64" | "u64"
	? u64 | i64
	: unsignedInt | signedInt;

export type numDataTypes = "i8" | "i16" | "i32" | "u8" | "u16" | "u32";

export type availableDataTypeForHash = numDataTypes | "u64" | "i64" | `str:${number}`;