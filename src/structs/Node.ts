import { END_DELIMITER, START_DELIMITER } from "../files/SST.js";
import { WAL_DELIMITER } from "../files/Wal.js";
import { FileDataType, WalMethod } from "../typings/enum.js";
import { IDataNodeOptions } from "../typings/interface.js";
import { dataType, PossibleKeyType } from "../typings/type.js";
import { checksum } from "../utils/checksum.js";
import {
	DataTypeToValue,
	ValueToBuffer,
	ValueToDataType,
	ValueToTypedArray,
	getDataTypeByteLength,
	getDataTypeFromEnum,
	getEnumKeyFromDataType,
} from "../utils/dataType.js";

export default class DataNode {
	#key: PossibleKeyType;
	#value: unknown;
	#keyType: dataType;
	#valueType: dataType;
	#offset: number = 0;
	#delete: boolean;
	#checksum: Uint8Array | Buffer;
	#length: number;
	#timestamp: bigint;
	constructor(options: IDataNodeOptions) {
		this.#key = options.key;
		this.#value = options.value;
		this.#keyType = options.keyType;
		this.#valueType = options.valueType;
		this.#offset = options.offset;
		this.#delete = options.delete;
		this.#checksum = options.checksum;
		this.#length = options.length;
		this.#timestamp = options.timestamp;
	}

	toUint8Array() {
		const buffer = new Uint8Array(this.#length);
		const dataView = new DataView(buffer.buffer);

		let offset = 0;
		const keyBuffer = ValueToDataType(this.#key, this.#keyType);
		const valueBuffer = ValueToDataType(this.#value, this.#valueType);

		// Set the start delimiter
		buffer.set(START_DELIMITER, offset);
		offset += 4;

		// Set key length
		dataView.setUint32(offset, keyBuffer.byteLength, true);
		offset += 4;

		// Set value length
		dataView.setUint32(offset, valueBuffer.byteLength, true);
		offset += 4;

		// Set checksum length
		dataView.setUint32(offset, this.#checksum.byteLength, true);
		offset += 4;

		// Set key data
		buffer.set(keyBuffer, offset);
		offset += keyBuffer.byteLength;

		// Set value data
		buffer.set(valueBuffer, offset);
		offset += valueBuffer.byteLength;

		// Set checksum data
		buffer.set(this.#checksum, offset);
		offset += this.#checksum.byteLength;

		// Set timestamp
		dataView.setBigUint64(offset, this.#timestamp, true);
		offset += 8;

		// Set delete flag
		buffer[offset++] = this.#delete ? 0x01 : 0x00;

		// Set the end delimiter
		buffer.set(END_DELIMITER, offset);

		return buffer;
	}

	toWAL() {
		return [
			this.#keyType,
			this.#valueType,
			this.#key,
			this.#value,
			this.#timestamp,
		].join(WAL_DELIMITER);
	}

	get key() {
		return this.#key;
	}

	get value() {
		return this.#value;
	}

	get keyType() {
		return this.#keyType;
	}

	get valueType() {
		return this.#valueType;
	}

	get offset() {
		return this.#offset;
	}

	get delete() {
		return this.#delete;
	}

	set delete(value: boolean) {
		this.#delete = value;
	}

	get checksum() {
		return this.#checksum;
	}

	get length() {
		return this.#length;
	}

	get timestamp() {
		return this.#timestamp;
	}

	get data() {
		return {
			key: this.#key,
			value: this.#value,
			keyType: this.#keyType,
			valueType: this.#valueType,
			offset: this.#offset,
			delete: this.#delete,
			checksum: this.#checksum,
			length: this.#length,
			timestamp: this.#timestamp,
		};
	}

	static Empty() {
		return new DataNode({
			key: "null",
			value: null,
			keyType: "u8",
			valueType: "u8",
			offset: -1,
			delete: false,
			checksum: new Uint8Array(32),
			length: 56,
			timestamp: BigInt(Date.now()),
		});
	}

	static deletedNode(key: PossibleKeyType, keyType: dataType,valueType: dataType) {
		return new DataNode({
			key,
			value: valueType.startsWith("str:") ? "1" : valueType === 'u64' || valueType === 'i64' ? 1n : 1,
			keyType,
			valueType: valueType,
			offset: -1,
			delete: true,
			checksum: new Uint8Array(32),
			length: 61 + getDataTypeByteLength(keyType)+getDataTypeByteLength(valueType),
			timestamp: BigInt(Date.now()),
		});
	
	}

	static fromUint8Array(
		line: Uint8Array,
		offset: number,
		options: { keyDataType: dataType; dataType: dataType }
	) {
		const kvPairLengthTotal = line.byteLength;
		const { keyDataType, dataType } = options;
		let offset_ = 4;

		const dataview = new DataView(line.buffer, offset_, 4);

		const keyLength = dataview.getUint32(0, true);
		offset_ += 4;

		const valueLength = dataview.getUint32(0, true);
		offset_ += 8;

		const actualKey = DataTypeToValue(
			line.slice(offset_, offset_ + keyLength),
			keyDataType
		);
		offset_ += keyLength;

		const actualValue = DataTypeToValue(
			line.slice(offset_, offset_ + valueLength),
			dataType
		);
		offset_ += valueLength;

		const checksum = line.slice(offset_, offset_ + 32);
		offset_ += 32;

		const timestamp = new DataView(line.buffer, offset_, 8).getBigUint64(
			0,
			true
		);
		offset_ += 8;

		const deleted = line[offset_] === 1;

		return new DataNode({
			key: actualKey,
			value: actualValue,
			keyType: keyDataType,
			valueType: dataType,
			offset,
			delete: deleted,
			checksum,
			length: kvPairLengthTotal,
			timestamp,
		});
	}

	static fromWAL(line: string) {
		const [keyType, valueType, key, value, timestamp, method] =
			line.split(WAL_DELIMITER);

		const keyBuffer = ValueToDataType(key, keyType as dataType);
		const valueBuffer = ValueToDataType(value, valueType as dataType);

		const checksumBuffer = new Uint8Array(
			keyBuffer.length + valueBuffer.length
		);
		checksumBuffer.set(keyBuffer, 0);
		checksumBuffer.set(valueBuffer, keyBuffer.length);

		return new DataNode({
			key: key,
			value: value,
			keyType: keyType as dataType,
			valueType: valueType as dataType,
			offset: -1,
			delete: Number(method) === WalMethod.Delete,
			checksum: checksum(checksumBuffer),
			length:
				61 +
				getDataTypeByteLength(keyType as dataType) +
				getDataTypeByteLength(valueType as dataType),
			timestamp: BigInt(timestamp),
		});
	}

	clone() {
		return new DataNode({
			key: this.#key,
			value: this.#value,
			keyType: this.#keyType,
			valueType: this.#valueType,
			offset: this.#offset,
			delete: this.#delete,
			checksum: this.#checksum,
			length: this.#length,
			timestamp: this.#timestamp,
		});
	}
}
