import { FileDataType } from "../typings/enum.js";
import { dataType, FileDataTypeMember, PossibleKeyType, TypedArray } from "../typings/type.js";

export function getDataTypeByteLength(type: dataType): number {
	if (type.startsWith("str:")) {
		return 2*parseInt(type.split(":")[1]);
	} else {
		switch (type) {
			case "i8":
			case "u8":
				return 1;
			case "i16":
			case "u16":
				return 2;
			case "i32":
			case "u32":
			case "f32":
				return 4;
			case "i64":
			case "u64":
			case "f64":
				return 8;
			case "bool":
				return 1;
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}
}

export function DataTypeToTypedArray(
	input: Uint8Array,
	type: dataType
): TypedArray {
	if (type.startsWith("str:")) {
		return input;
	} else {
		switch (type) {
			case "i8":
				return new Int8Array(input.buffer);
			case "i16":
				return new Int16Array(input.buffer);
			case "i32":
				return new Int32Array(input.buffer);
			case "i64":
				return new BigInt64Array(input.buffer);
			case "u8":
				return input;
			case "u16":
				return new Uint16Array(input.buffer);
			case "u32":
				return new Uint32Array(input.buffer);
			case "u64":
				return new BigUint64Array(input.buffer);
			case "f32":
				return new Float32Array(input.buffer);
			case "f64":
				return new Float64Array(input.buffer);
			case "bool":
				return input;
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}
}

export function TypedArrayToValue(input: TypedArray, type: dataType): any {
	if (type.startsWith("str:")) {
		return Buffer.from(input.buffer).toString();
	} else {
		return input[0];
	}
}


export function DataTypeToValue(input: Uint8Array, type: dataType): PossibleKeyType {
	if (type.startsWith("str:")) {
		return Buffer.from(input).toString();
	} 

	switch (type) {
		case "i8":
			return new Int8Array(input.buffer)[0];
		case "i16":
			return new Int16Array(input.buffer)[0];
		case "i32":
			return new Int32Array(input.buffer)[0];
		case "i64":
			return new BigInt64Array(input.buffer)[0];
		case "u8":
			return new Uint8Array(input.buffer)[0];
		case "u16":
			return new Uint16Array(input.buffer)[0];
		case "u32":
			return new Uint32Array(input.buffer)[0];
		case "u64":
			return new BigUint64Array(input.buffer)[0];
		case "f32":
			return new Float32Array(input.buffer)[0];
		case "f64":
			return new Float64Array(input.buffer)[0];
		case "bool":
			return new Uint8Array(input.buffer)[0];
		default:
			throw new Error(`Invalid data type: ${type}`);
	
	}
}
export function ValueToTypedArray(input: any, type: dataType): TypedArray {
	if (type.startsWith("str:")) {
		return new TextEncoder().encode(input);
	} else {
		switch (type) {
			case "i8":
				return new Int8Array([input]);
			case "i16":
				return new Int16Array([input]);
			case "i32":
				return new Int32Array([input]);
			case "i64":
				return new BigInt64Array([input]);
			case "u8":
				return new Uint8Array([input]);
			case "u16":
				return new Uint16Array([input]);
			case "u32":
				return new Uint32Array([input]);
			case "u64":
				return new BigUint64Array([input]);
			case "f32":
				return new Float32Array([input]);
			case "f64":
				return new Float64Array([input]);
			case "bool":
				return new Uint8Array([input]);
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}
}


export function ValueToDataType(input: unknown, type: dataType): Uint8Array {
	if (type.startsWith("str:")) {
		return new TextEncoder().encode(input as string);
	} else {
		switch (type) {
			case "i8":
				return new Uint8Array([input as number])
			case "i16": {
				// maths
				const highbits = (input as number) >> 8;
				const llowbits = (input as number) & 0xff;
				return new Uint8Array([highbits, llowbits]);
			}
			case "i32": {
				const highestbits = (input as number) >> 24;
				const highbits = ((input as number) >> 16) & 0xff;
				const lowbits = ((input as number) >> 8) & 0xff;
				const lowestbits = (input as number) & 0xff;
				return new Uint8Array([highestbits, highbits, lowbits, lowestbits]);
			}
			case "i64": {
				const bigint = BigInt(input as number);
				const highestbits = Number(bigint >> 56n);
				const highbits = Number((bigint >> 48n) & 0xffn);
				const highlowbits = Number((bigint >> 40n) & 0xffn);
				const highlowestbits = Number((bigint >> 32n) & 0xffn);
				const lowhighestbits = Number((bigint >> 24n) & 0xffn);
				const lowhighbits = Number((bigint >> 16n) & 0xffn);
				const lowlowbits = Number((bigint >> 8n) & 0xffn);
				const lowestbits = Number(bigint & 0xffn);
				return new Uint8Array([highestbits, highbits, highlowbits, highlowestbits, lowhighestbits, lowhighbits, lowlowbits, lowestbits]);
			}
			case "u8":
				return new Uint8Array([input as number]);
			case "u16": {
				const highbits = (input as number) >> 8;
				const llowbits = (input as number) & 0xff;
				return new Uint8Array([highbits, llowbits]);
			}
			case "u32": {
				const highestbits = (input as number) >> 24;
				const highbits = ((input as number) >> 16) & 0xff;
				const lowbits = ((input as number) >> 8) & 0xff;
				const lowestbits = (input as number) & 0xff;
				return new Uint8Array([highestbits, highbits, lowbits, lowestbits]);
			}
			case "u64": {
				const bigint = BigInt(input as number);
				const highestbits = Number(bigint >> 56n);
				const highbits = Number((bigint >> 48n) & 0xffn);
				const highlowbits = Number((bigint >> 40n) & 0xffn);
				const highlowestbits = Number((bigint >> 32n) & 0xffn);
				const lowhighestbits = Number((bigint >> 24n) & 0xffn);
				const lowhighbits = Number((bigint >> 16n) & 0xffn);
				const lowlowbits = Number((bigint >> 8n) & 0xffn);
				const lowestbits = Number(bigint & 0xffn);
				return new Uint8Array([highestbits, highbits, highlowbits, highlowestbits, lowhighestbits, lowhighbits, lowlowbits, lowestbits]);
			}
			case "f32": {
				return new Uint8Array(new Float32Array([input as number]).buffer);
			}
			case "f64": {
				return new Uint8Array(new Float64Array([input as number]).buffer);
			}
			case "bool":
				return new Uint8Array([input as number]);
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}
}

export function ValueToBuffer(input: any, type: dataType): Buffer {
	return Buffer.from(ValueToTypedArray(input, type).buffer);

}

export function getEnumKeyFromDataType(type: dataType): keyof typeof FileDataType {
	if (type.startsWith("str:")) {
		return "Str";
	} else {
		switch (type) {
			case "i8":
				return "I8";
			case "i16":
				return "I16";
			case "i32":
				return "I32";
			case "i64":
				return "I64";
			case "u8":
				return "U8";
			case "u16":
				return "U16";
			case "u32":
				return "U32";
			case "u64":
				return "U64";
			case "f32":
				return "F32";
			case "f64":
				return "F64";
			case "bool":
				return "Bool";
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}
}

export function get4pointRangeOfDataType(type: dataType) {
	if (type === "bool")
		throw new Error("dataType bool cannot be used for key range");
	let min: number | bigint;
	let max: number | bigint;
	if (type.startsWith("str:")) {
		min = 0;
		max = 65535;
	} else {
		switch (type) {
			case "i8":
				min = -128;
				max = 127;
				break;
			case "i16":
				min = -32768;
				max = 32767;
				break;
			case "i32":
				min = -2147483648;
				max = 2147483647;
				break;
			case "i64":
				min = -9223372036854775808n;
				max = 9223372036854775807n;
				break;
			case "u8":
				min = 0;
				max = 255;
				break;
			case "u16":
				min = 0;
				max = 65535;
				break;
			case "u32":
				min = 0;
				max = 4294967295;
				break;
			case "u64":
				min = 0n;
				max = 18446744073709551615n;
				break;
			case "f32":
				min = -3.4028234663852886e38;
				max = 3.4028234663852886e38;
				break;
			case "f64":
				min = -1.7976931348623157e308;
				max = 1.7976931348623157e308;
				break;
			default:
				throw new Error(`Invalid data type: ${type}`);
		}
	}

	const ans: (number | bigint)[] = [];
	const parts =
		typeof min === "bigint"
			? (min + ((max as bigint) - min) / 2n) / 2n
			: (min + ((max as number) - min) / 2) / 2;

	for (let i = 0; i < 4; i++) {
		ans.push(typeof parts === "bigint" ? parts * BigInt(i) : parts * i);
	}

	return ans;
}


export function getDataTypeFromEnum(type:FileDataType,keyLength?:number) {
	switch (type) {
		case FileDataType.Bool:
			return "bool";
		case FileDataType.F32:
			return "f32";
		case FileDataType.F64:
			return "f64";
		case FileDataType.I16:
			return "i16";
		case FileDataType.I32:
			return "i32";
		case FileDataType.I64:
			return "i64";
		case FileDataType.I8:
			return "i8";
		case FileDataType.Str:
			return `str:${keyLength!}`;
		case FileDataType.U16:
			return "u16";
		case FileDataType.U32:
			return "u32";
		case FileDataType.U64:
			return "u64";
		case FileDataType.U8:
			return "u8";
		default:
			throw new Error(`Invalid data type: ${type}`);
	}
}

export function timestampToUint8ArrayLE(timestampInMs:number) {
    // Special case for zero
    if (timestampInMs === 0) {
        return new Uint8Array(8);
    }

    const exponent = Math.floor(Math.log2(timestampInMs));
    const mantissa = timestampInMs / Math.pow(2, exponent) - 1;

    const biasedExponent = exponent + 1023;

    const mantissaHigh = Math.floor(mantissa * Math.pow(2, 20));
    const mantissaLow = Math.floor((mantissa * Math.pow(2, 52)) % Math.pow(2, 32));

    const highBits = (biasedExponent << 20) | mantissaHigh;
    const lowBits = mantissaLow;

    return [
        lowBits & 0xFF,
		(lowBits >> 8) & 0xFF,
		(lowBits >> 16) & 0xFF,
		(lowBits >> 24) & 0xFF,
		highBits & 0xFF,
		(highBits >> 8) & 0xFF,
		(highBits >> 16) & 0xFF,
		(highBits >> 24) & 0xFF,
    ];
}