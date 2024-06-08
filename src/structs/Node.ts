import { BASE_BYTES, END_DELIMITER, START_DELIMITER } from "../files/SST.js";
import { WAL_START_DELIMITER, WAL_END_DELIMITER } from "../files/Wal.js";
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
	timestampToUint8ArrayLE,
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
	#timestamp: number;
	#dataBuffer: Uint8Array;
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
		this.#dataBuffer = options.dataBuffer;
	}

	toUint8Array() {
		const buffer = new Uint8Array(this.#length);
		const keyTypeLength = getDataTypeByteLength(this.#keyType);
		const valueTypeLength = getDataTypeByteLength(this.#valueType);
		let offset = 0;

		// Set the start delimiter
		buffer.set(START_DELIMITER, offset);
		offset += 4;

		//convert keyLength from u32 to u8[4] - if u are wondering why i did this, because this added one more 0 to ops :)
		let highestbits = keyTypeLength >> 24
		let highbits = keyTypeLength >> 16 & 0xff
		let lowbits = keyTypeLength >> 8 & 0xff
		let lowestbits = keyTypeLength & 0xff
		buffer[offset++] = lowestbits
		buffer[offset++] = lowbits
		buffer[offset++] = highbits
		buffer[offset++] = highestbits
		
		//convert valueLength from u32 to u8[4] 
		highestbits = valueTypeLength >> 24
		highbits = valueTypeLength >> 16 & 0xff
		lowbits = valueTypeLength >> 8 & 0xff
		lowestbits = valueTypeLength & 0xff
		buffer[offset++] = lowestbits
		buffer[offset++] = lowbits
		buffer[offset++] = highbits
		buffer[offset++] = highestbits

		// Set checksum length
		buffer[offset++] = 4;
		buffer[offset++] = 0;
		buffer[offset++] = 0;
		buffer[offset++] = 0;
		
		// Set key data
		for(let i = 0; i < this.#dataBuffer.length; i++) {
			buffer[offset++] = this.#dataBuffer[i];
		}
		
		// Set checksum data
		buffer[offset++] = this.#checksum[0];
		buffer[offset++] = this.#checksum[1];
		buffer[offset++] = this.#checksum[2];
		buffer[offset++] = this.#checksum[3];

		// Set timestamp
		const timestampu8s = timestampToUint8ArrayLE(this.#timestamp);
		buffer[offset++] = timestampu8s[0];
		buffer[offset++] = timestampu8s[1];
		buffer[offset++] = timestampu8s[2];
		buffer[offset++] = timestampu8s[3];
		buffer[offset++] = timestampu8s[4];
		buffer[offset++] = timestampu8s[5];
		buffer[offset++] = timestampu8s[6];
		buffer[offset++] = timestampu8s[7];
		
		// Set delete flag
		buffer[offset++] = this.#delete ? 0x01 : 0x00;

		// Set the end delimiter
		buffer.set(END_DELIMITER, offset);
		return buffer;
	}

	toWAL() {
		const walBuffer = new Uint8Array(18 + this.#dataBuffer.byteLength);
		let offset = 0;
		
		walBuffer[offset++] = FileDataType[getEnumKeyFromDataType(this.#keyType)] as number;
		walBuffer[offset++] = FileDataType[getEnumKeyFromDataType(this.#valueType)];
			
	
		// set keylength
		const keyLen = getDataTypeByteLength(this.#keyType);
		let highestbits = keyLen >> 24
		let highbits = keyLen >> 16 & 0xff
		let lowbits = keyLen >> 8 & 0xff
		let lowestbits = keyLen & 0xff
		
		walBuffer[offset++] = lowestbits;
		walBuffer[offset++] = lowbits;
		walBuffer[offset++] = highbits;
		walBuffer[offset++] = highestbits;
		
		
		// set valuelength
		const valueLen = getDataTypeByteLength(this.#valueType);
		highestbits = valueLen >> 24
		highbits = valueLen >> 16 & 0xff
		lowbits = valueLen >> 8 & 0xff
		lowestbits = valueLen & 0xff

		walBuffer[offset++] = lowestbits;
		walBuffer[offset++] = lowbits;
		walBuffer[offset++] = highbits;
		walBuffer[offset++] = highestbits;

		for (let i = 0; i < this.#dataBuffer.length; i++) {
			walBuffer[offset++] = this.#dataBuffer[i];
		}
		
		const timestampu8s = timestampToUint8ArrayLE(this.#timestamp);
		walBuffer[offset++] = timestampu8s[0];
		walBuffer[offset++] = timestampu8s[1];
		walBuffer[offset++] = timestampu8s[2];
		walBuffer[offset++] = timestampu8s[3];
		walBuffer[offset++] = timestampu8s[4];
		walBuffer[offset++] = timestampu8s[5];
		walBuffer[offset++] = timestampu8s[6];
		walBuffer[offset++] = timestampu8s[7];

		return walBuffer;
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

	get dataBuffer() {
		return this.#dataBuffer;
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
			dataBuffer: this.#dataBuffer,
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
			checksum: new Uint8Array(4),
			length: 56,
			timestamp: Date.now(),
			dataBuffer: new Uint8Array(0),
		});
	}

	static deletedNode(
		key: PossibleKeyType,
		keyType: dataType,
		valueType: dataType
	) {
		return new DataNode({
			key,
			value: valueType.startsWith("str:")
				? "1"
				: valueType === "u64" || valueType === "i64"
				? 1n
				: 1,
			keyType,
			valueType: valueType,
			offset: -1,
			delete: true,
			checksum: new Uint8Array(4),
			length:
				BASE_BYTES +
				getDataTypeByteLength(keyType) +
				getDataTypeByteLength(valueType),
			timestamp: Date.now(),
			dataBuffer: new Uint8Array(0),
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

		const dataView = new DataView(line.buffer);
		const keyTypeLength = dataView.getUint32(offset_, true);
		offset_ += 4;

		const valueTypeLength = dataView.getUint32(offset_, true);
		offset_ += 4;

		const checksumLength = dataView.getUint32(offset_, true);
		offset_ += 4;

		const keyBuffer = line.slice(offset_, offset_ + keyTypeLength);
		offset_ += keyTypeLength;

		const valueBuffer = line.slice(offset_, offset_ + valueTypeLength);
		offset_ += valueTypeLength;

		const checksum = line.slice(offset_, offset_ + checksumLength);
		offset_ += checksumLength;

		const timestamp = dataView.getFloat64(offset_, true);
		offset_ += 8;

		const deleted = line[offset_++] === 0x01;

		const actualKey = DataTypeToValue(keyBuffer, keyDataType);
		const actualValue = DataTypeToValue(valueBuffer, dataType);

		const dataBuffer = new Uint8Array(
			keyBuffer.length + valueBuffer.length
		);
		dataBuffer.set(keyBuffer);
		dataBuffer.set(valueBuffer, keyBuffer.length);

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
			dataBuffer,
		});
	}

	static fromWAL(line: Uint8Array) {
		const dataView = new DataView(line.buffer);
		let offset = 6;

		const keyLength = dataView.getUint32(offset, true);
		offset += 4;
		const valueLength = dataView.getUint32(offset, true);
		offset += 4;

		const dataBuffer = line.slice(offset, offset + keyLength + valueLength);
		offset += keyLength + valueLength;

		const timestamp = dataView.getFloat64(offset, true);
		offset += 8;

		const method = line[offset++];

		const keyType = getDataTypeFromEnum(line[4], keyLength) as dataType;
		const valueType = getDataTypeFromEnum(line[5], valueLength) as dataType;

		const key = DataTypeToValue(dataBuffer.slice(0, keyLength), keyType);
		const value = DataTypeToValue(dataBuffer.slice(keyLength), valueType);

		return new DataNode({
			key: key,
			value: value,
			keyType: keyType as dataType,
			valueType: valueType as dataType,
			offset: -1,
			delete: Number(method) === WalMethod.Delete,
			checksum: checksum(dataBuffer),
			length:
				BASE_BYTES +
				getDataTypeByteLength(keyType as dataType) +
				getDataTypeByteLength(valueType as dataType),
			timestamp: Number(timestamp),
			dataBuffer,
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
			checksum: new Uint8Array(this.#checksum),
			length: this.#length,
			timestamp: this.#timestamp,
			dataBuffer: new Uint8Array(this.#dataBuffer),
		});
	}
}
