import fsp from "node:fs/promises";
import Btree from "sorted-btree";
import mmap from "@raygun-nickj/mmap-io";

import {
	CompressionFlag,
	EncodingFlag,
	FileDataType,
} from "../typings/enum.js";
import {
	ISSTFileOptions,
	IHeaderData,
	IMetadata,
} from "../typings/interface.js";
import { DeepRequired, PossibleKeyType } from "../typings/type.js";
import {
	DataTypeToValue,
	getDataTypeByteLength,
	getEnumKeyFromDataType,
	ValueToDataType,
} from "../utils/dataType.js";
import NyaDBError from "../misc/Error.js";
import { checksum, validateChecksum } from "../utils/checksum.js";
import DataNode from "../structs/Node.js";
import { BloomFilter } from "../structs/Filters.js";
import { getCellAndHashCount } from "../utils/bloom.js";
import BufferNode from "../structs/BufferNode.js";

export const HEADER = 7;
export const METADATA = 3;
export const SST_TABLE_MAGIC_NUMBER = [0x53, 0x53, 0x54, 0x54];
export const SUPPORTED_VERSION = [1];
export const KVS_PER_PAGE = 1000;
export const START_DELIMITER = new Uint8Array([0x53, 0x54, 0x41, 0x52]);
export const END_DELIMITER = new Uint8Array([0x45, 0x4e, 0x44, 0x45]);

export default class SSTFile {
	#options: DeepRequired<ISSTFileOptions>;
	#btree: Btree.default<PossibleKeyType, number> = new Btree.default(
		undefined,
		(a, b) => {
			if (a < b) return -1;
			if (a > b) return 1;
			return 0;
		}
	);
	#headerData!: IHeaderData;
	#metaData!: IMetadata;
	#bloomFilter!: BloomFilter;
	#fileHandle!: fsp.FileHandle;
	#bloomFileHandle!: fsp.FileHandle;
	#indexFileHandle!: fsp.FileHandle;
	#fileSize: number = -1;
	#lastOffset: number = 13;
	#mmap!: Buffer | null;
	#lowerPair: [PossibleKeyType, number] = [1, -1];

	constructor(options: ISSTFileOptions) {
		this.#options = this.#finalizeOptions(options);
		const [bitCount, hashCount] = getCellAndHashCount(
			this.#options.kvCount,
			0.2
		);
		this.#bloomFilter = new BloomFilter(bitCount, hashCount);
	}

	#defaultOptions: ISSTFileOptions = {
		path: "./database/column/level/main.sst",
		dataType: "str:1024",
		keyDataType: "u64",
		encoding: EncodingFlag.None,
		compression: CompressionFlag.None,
		kvCount: 10000,
		doBatchValidation: false,
		kvPerPage: 1000,
	};

	#finalizeOptions(options: ISSTFileOptions): DeepRequired<ISSTFileOptions> {
		return {
			...this.#defaultOptions,
			...options,
		};
	}

	async #generateFileInfo() {
		/*
Header Length (1 bytes)       
| Header                        |
| +---------------------------+ |
| | Magic Number (4 bytes)    | |
| | Version Flag (1 byte)     | |
| | Compression Flag (1 byte) | |
| | Encoding Flag (1 byte)    | |
| +---------------------------+ |
Metadata Length (1 bytes)
| Metadata                     |
| +---------------------------+ |
| | Value Data Type (1 byte) | |
| | Key Data Type (1 byte)   | |
| | KVPair Length (1 byte)   | |
| +---------------------------+ |
| | STARTDELIMITER (4 bytes ) | key length (4 bytes) | value length (4 bytes) | checksum length (4 bytes) | key (key length) | value (value length) | checksum (32 bytes ) | timestamp (8 bytes) | deleted (1 byte) | ENDDELIMITER (4 bytes) | (repeat for KVS_PER_PAGE times) ||
*/

		const bufferArray = new Uint8Array(13);

		let offset = 0;
		bufferArray.set(new Uint8Array([HEADER]), offset);
		offset++;

		bufferArray.set(new Uint8Array(SST_TABLE_MAGIC_NUMBER), offset);
		offset += 4;

		bufferArray[offset++] = SUPPORTED_VERSION.at(-1)!; // version flag
		bufferArray[offset++] = this.#options.compression;
		bufferArray[offset++] = this.#options.encoding;

		this.#headerData = {
			magicNumber: "0x434F4C4E",
			versionFlag: 1,
			compressionFlag: this.#options.compression,
			encodingFlag: this.#options.encoding,
		};

		bufferArray.set(new Uint8Array([METADATA]), offset);
		offset++;

		bufferArray[offset++] =
			FileDataType[getEnumKeyFromDataType(this.#options.dataType)];
		bufferArray[offset++] =
			FileDataType[getEnumKeyFromDataType(this.#options.keyDataType)];

		const kvPairLength =
			61 +
			getDataTypeByteLength(this.#options.dataType) +
			getDataTypeByteLength(this.#options.keyDataType);
		bufferArray[offset++] = kvPairLength;

		this.#metaData = {
			valueDataType: this.#options.dataType,
			keyDataType: this.#options.keyDataType,
			kvPairLength: {
				key: getDataTypeByteLength(this.#options.keyDataType),
				value: getDataTypeByteLength(this.#options.dataType),
				checksum: 32,
				total: kvPairLength,
			},
		};

		// add a new line character
		bufferArray[offset++] = 0x0a;

		await fsp.writeFile(this.#options.path, bufferArray);
	}

	async #verifyFileInfo() {
		const headerLengthBuffer = new Uint8Array(1);
		let offset = 0;
		await this.#fileHandle.read(headerLengthBuffer, 0, 1, 0);
		const headerLength = Math.min(headerLengthBuffer[0], HEADER);
		offset++;

		const headerBuffer = new Uint8Array(headerLength);
		await this.#fileHandle.read(headerBuffer, 0, headerLength, offset);
		offset += headerLength;

		const magicNumber = headerBuffer.slice(0, 4);
		if (
			!SST_TABLE_MAGIC_NUMBER.every(
				(value, index) => value === magicNumber[index]
			)
		) {
			throw new NyaDBError.InitError(
				`Invalid Magic Number, File is not a valid Column File`,
				`open`,
				this
			);
		}

		const versionFlag = headerBuffer[4];
		if (!SUPPORTED_VERSION.includes(versionFlag)) {
			throw new NyaDBError.InitError(
				`Unsupported Version Flag: ${versionFlag}`,
				`open`,
				this
			);
		}

		const compressionFlag = headerBuffer[5];
		if (compressionFlag !== this.#options.compression) {
			throw new NyaDBError.InitError(
				`Compression Flag Mismatch: expected ${
					this.#options.compression
				}, got ${compressionFlag}`,
				`open`,
				this
			);
		}

		const encodingFlag = headerBuffer[6];
		if (encodingFlag !== this.#options.encoding) {
			throw new NyaDBError.InitError(
				`Encoding Flag Mismatch: expected ${
					this.#options.encoding
				}, got ${encodingFlag}`,
				`open`,
				this
			);
		}

		this.#headerData = {
			magicNumber: `0x${magicNumber[0].toString(
				16
			)}${magicNumber[1].toString(16)}${magicNumber[2].toString(
				16
			)}${magicNumber[3].toString(16)}`,
			versionFlag: versionFlag,
			compressionFlag: compressionFlag,
			encodingFlag: encodingFlag,
		};

		const metadataLengthBuffer = new Uint8Array(1);
		await this.#fileHandle.read(metadataLengthBuffer, 0, 1, offset);
		const metadataLength = Math.min(metadataLengthBuffer[0], METADATA);
		offset++;

		const metadataBuffer = new Uint8Array(metadataLength);
		await this.#fileHandle.read(metadataBuffer, 0, metadataLength, offset);
		offset += metadataLength + 1;

		if (
			metadataBuffer[0] !==
			FileDataType[getEnumKeyFromDataType(this.#options.dataType)]
		) {
			throw new NyaDBError.InitError(
				`Data Type Mismatch: expected ${
					this.#options.dataType
				}, got ${FileDataType[metadataBuffer[0]].toLowerCase()}`,
				`open`,
				this
			);
		}

		if (
			metadataBuffer[1] !==
			FileDataType[getEnumKeyFromDataType(this.#options.keyDataType)]
		) {
			throw new NyaDBError.InitError(
				`Key Data Type Mismatch: expected ${
					this.#options.keyDataType
				}, got ${FileDataType[metadataBuffer[1]].toLowerCase()}`,
				`open`,
				this
			);
		}

		if (
			metadataBuffer[2] !==
			61 +
				getDataTypeByteLength(this.#options.dataType) +
				getDataTypeByteLength(this.#options.keyDataType)
		) {
			throw new NyaDBError.InitError(
				`KVPair Length Mismatch: expected ${
					57 +
					getDataTypeByteLength(this.#options.dataType) +
					getDataTypeByteLength(this.#options.keyDataType)
				}, got ${metadataBuffer[2]}`,
				`open`,
				this
			);
		}

		this.#metaData = {
			valueDataType: this.#options.dataType,
			keyDataType: this.#options.keyDataType,
			kvPairLength: {
				key: getDataTypeByteLength(this.#options.keyDataType),
				value: getDataTypeByteLength(this.#options.dataType),
				checksum: 32,
				total:
					61 +
					getDataTypeByteLength(this.#options.dataType) +
					getDataTypeByteLength(this.#options.keyDataType),
			},
		};

		if (this.#options.doBatchValidation) {
			this.#batchVerifyChecksum(offset);
		}
	}

	async #batchVerifyChecksum(offset: number) {
		const promises = [];
		for (
			let i = offset;
			i < this.#fileSize;
			i += KVS_PER_PAGE * this.#metaData.kvPairLength.total
		) {
			promises.push(this.#validateChecksum(i));
		}

		await Promise.all(promises);
	}

	async #validateChecksum(offset_: number) {
		let first = false;
		const line = new Uint8Array(this.#metaData.kvPairLength.total);
		for (
			let i = offset_;
			i <
			Math.min(
				offset_ + KVS_PER_PAGE * this.#metaData.kvPairLength.total,
				this.#fileSize
			);
			i += this.#metaData.kvPairLength.total
		) {
			await this.#fileHandle.read(
				line,
				0,
				this.#metaData.kvPairLength.total,
				i
			);

			let offset = 0;
			const startDelimiter = line.slice(offset, offset + 4);
			offset += 4;

			const keyLength = new Uint32Array(
				line.slice(offset, offset + 4)
			)[0];
			offset += 4;

			const valueLength = new Uint32Array(
				line.slice(offset, offset + 4)
			)[0];
			offset += 4;

			const checksumLength = new Uint32Array(
				line.slice(offset, offset + 4)
			)[0];
			offset += 4;

			const data = line.slice(offset, offset + keyLength + valueLength);
			offset += keyLength + valueLength;

			const checksum_ = line.slice(offset, offset + checksumLength);
			offset += checksumLength;

			offset += 8;

			const deleted = line[offset];
			offset++;

			if (deleted === 1) continue;

			const endLimiter = line.slice(offset, offset + 4);

			if (
				!startDelimiter.every(
					(value, index) => value === START_DELIMITER[index]
				)
			) {
				throw new NyaDBError.InitError(
					`Invalid Start Delimiter: expected ${START_DELIMITER}, got ${startDelimiter}`,
					`open`,
					this
				);
			}

			if (
				!endLimiter.every(
					(value, index) => value === END_DELIMITER[index]
				)
			) {
				throw new NyaDBError.InitError(
					`Invalid End Delimiter: expected ${END_DELIMITER}, got ${endLimiter}`,
					`open`,
					this
				);
			}

			if (!validateChecksum(data, checksum_)) {
				throw new NyaDBError.InitError(
					`Invalid Checksum: expected ${checksum_.join(
						" ,"
					)}, got ${checksum(data).join(" ,")}`,
					`open`,
					this
				);
			}
		}
	}

	async #readFromOffset(offset: number) {
		const kvPairLengthTotal = this.#metaData.kvPairLength.total;

		const line = new Uint8Array(kvPairLengthTotal);
		await this.#fileHandle.read(line, 0, kvPairLengthTotal, offset);

		return this.#convertUint8ArrayToDataNode(line, offset);
	}

	#convertUint8ArrayToDataNode(line: Uint8Array, offset: number): BufferNode {
		return new BufferNode(
			line,
			offset,
			this.#metaData.keyDataType,
			this.#metaData.valueDataType
		);

		// 		const kvPairLengthTotal = this.#metaData.kvPairLength.total;
		// 		const { keyDataType, dataType } = this.#options;
		// 		let offset_ = 4;

		// 		const dataview = new DataView(line.buffer, offset_, 4);

		// 		const keyLength = dataview.getUint32(0, true);
		// 		offset_ += 4;

		// 		const valueLength = dataview.getUint32(0, true);
		// 		offset_ += 4;

		// 		const actualKey = DataTypeToValue(
		// 			line.slice(offset_, offset_ + keyLength),
		// 			keyDataType
		// 		);
		// 		offset_ += keyLength;

		// 		const actualValue = DataTypeToValue(
		// 			line.slice(offset_, offset_ + valueLength),
		// 			dataType
		// 		);
		// 		offset_ += valueLength;

		// 		const checksum = line.slice(offset_, offset_ + 32);
		// 		offset_ += 32;

		// 		const timestamp = new DataView(line.buffer, offset_, 8).getBigUint64(
		// 			0,
		// 			true
		// 		);
		// 		offset_ += 8;

		// 		const deleted = line[offset_] === 1;
		//  // @ts-ignore
		// 		return new DataNode({
		// 			key: actualKey,
		// 			value: actualValue,
		// 			keyType: keyDataType,
		// 			valueType: dataType,
		// 			offset,
		// 			delete: deleted,
		// 			checksum,
		// 			length: kvPairLengthTotal,
		// 			timestamp,
		// 		});
	}

	async #buildIndexTree(
		data: Uint8Array[],
		append: boolean = false,
		baseOffset = 0
	) {
		let base = append ? baseOffset : 3 + HEADER + METADATA;
		if (!append) {
			this.#bloomFilter.clear();
			this.#btree.clear();
		} else {
			this.#options.kvCount += data.length;
			const [bitCount, hashCount] = getCellAndHashCount(
				this.#options.kvCount,
				0.2
			);
			// copy the old bloom filter into new array
			const newArray = new Array(bitCount).fill(0);
			this.#bloomFilter.bits.forEach((bit, index) => {
				newArray[index] = bit;
			});
			this.#bloomFilter.setBits(newArray);
			this.#bloomFilter.setHashCount(hashCount);
		}
		// save every KVS_PER_PAGEth key
		for (let i = 0; i < data.length; i += this.options.kvPerPage) {
			const key = this.#getKeyFromUint8Array(data[i]);
			this.#btree.set(key, base);
			base += this.options.kvPerPage * this.#metaData.kvPairLength.total;
		}

		for (const d of data) {
			this.#bloomFilter.add(this.#getKeyFromUint8Array(d).toString());
		}

		// save this to .index file
		await this.#indexFileHandle.write(this.#btree.toString(), 0, "utf-8");
		// save this to .bloom file
		await this.#bloomFileHandle.write(
			this.#bloomFilter.bits.toString(),
			0,
			"utf-8"
		);
	}

	#getKeyFromUint8Array(line: Uint8Array) {
		let offset = 4;

		const keyLength = new Uint32Array(line.slice(offset, offset + 4))[0];
		offset += 12;
		const key = line.slice(offset, offset + keyLength);

		return DataTypeToValue(key, this.#metaData.keyDataType);
	}

	async #loadBloomFilter() {
		const data = (await this.#bloomFileHandle.readFile()).toString();
		if (data.trim().length === 0) return;

		const bits = data.split(",");

		this.#bloomFilter.setBits(bits.map((bit) => parseInt(bit)));
	}

	async #loadIndexTree() {
		const data = (await this.#indexFileHandle.readFile()).toString();
		if (data.length === 0) return;

		const arr = data.split(",");

		for (let i = 0; i < arr.length; i += 2) {
			const key = this.#options.keyDataType.startsWith("str:")
				? arr[i]
				: parseInt(arr[i]);
			this.#btree.set(key, parseInt(arr[i + 1]));
		}
	}

	async open() {
		this.#fileHandle = await fsp.open(
			this.#options.path,
			fsp.constants.O_RDWR | fsp.constants.O_CREAT
		);

		this.#bloomFileHandle = await fsp.open(
			`${this.#options.path.replace(".sst", "")}.bloom`,
			fsp.constants.O_RDWR | fsp.constants.O_CREAT
		);

		await this.#loadBloomFilter();

		this.#indexFileHandle = await fsp.open(
			`${this.#options.path.replace(".sst", "")}.index`,
			fsp.constants.O_RDWR | fsp.constants.O_CREAT
		);

		await this.#loadIndexTree();

		this.#fileSize = (await this.#fileHandle.stat()).size;

		if (this.#fileSize === 0) {
			await this.#generateFileInfo();
		} else {
			await this.#verifyFileInfo();
		}

		const fd = this.#fileHandle.fd;
		this.#fileSize = (await this.#fileHandle.stat()).size;
		// @ts-ignore
		let bestPageSize = Math.floor(this.#fileSize / mmap.PAGESIZE);
		// @ts-ignore
		if (this.#fileSize % mmap.PAGESIZE !== 0) {
			bestPageSize++;
		}
		try {
			// @ts-ignore
			this.#mmap = mmap.map(
				// @ts-ignore
				mmap.PAGESIZE * bestPageSize,
				// @ts-ignore
				mmap.PROT_READ,
				// @ts-ignore
				mmap.MAP_SHARED,
				fd,
				0
			);
		} catch (e) {
			console.error(
				"Mmap creation failed with error:",
				e,
				"Switching to read from file"
			);
		}
	}

	async close() {
		await this.#fileHandle.close();
		// delete mmap
		this.#mmap = null;
	}

	async readKey(key: PossibleKeyType) {
		// get the lower bound of the key from the btree
		const lb = this.#btree.nextLowerPair(key, this.#lowerPair);

		if (!lb) return null;

		const offset = lb[1];

		if (offset === -1) return null;

		// read the whole KVS_PER_PAGE chunk
		const chunk = new Uint8Array(
			Math.min(
				KVS_PER_PAGE * this.#metaData.kvPairLength.total,
				this.#fileSize - offset
			)
		);
		await this.#fileHandle.read(chunk, 0, chunk.length, offset);

		// find the key in the chunk , the key is sorted so we can break the loop
		// binary search can be used here

		let found = false;
		let i = offset,
			j = chunk.length / this.#metaData.kvPairLength.total;

		while (i < j) {
			const mid = i + Math.floor((j - i) / 2);
			const offset_ = mid * this.#metaData.kvPairLength.total;
			const key = this.#getKeyFromUint8Array(
				chunk.slice(
					offset_,
					offset_ + this.#metaData.kvPairLength.total
				)
			);
			if (key === lb[0]) {
				found = true;
				break;
			}
			if (key < lb[0]) {
				i = mid + 1;
			} else {
				j = mid;
			}
		}

		if (!found) return null;

		const offset_ = i * this.#metaData.kvPairLength.total;
		const line = chunk.slice(
			offset_,
			offset_ + this.#metaData.kvPairLength.total
		);
		return this.#convertUint8ArrayToDataNode(line, offset_);
	}

	async readKeyMmap(key: PossibleKeyType) {
		// get the lower bound of the key from the btree
		if (!this.#mmap) {
			return this.readKey(key);
		}
		const lb = this.#btree.nextLowerPair(key, this.#lowerPair);

		if (!lb) return null;

		const offset = lb[1];

		if (offset === -1) return null;

		// read the whole KVS_PER_PAGE chunk
		const chunk = new Uint8Array(
			Math.min(
				KVS_PER_PAGE * this.#metaData.kvPairLength.total,
				this.#fileSize - offset
			)
		);

		this.#mmap!.copy(chunk, 0, offset, offset + chunk.length);

		// find the key in the chunk , the key is sorted so we can break the loop
		// binary search can be used here

		let found = false;
		let i = offset,
			j = chunk.length / this.#metaData.kvPairLength.total;
		while (i < j) {
			const mid = i + Math.floor((j - i) / 2);
			const offset_ = mid * this.#metaData.kvPairLength.total;

			const key_ = this.#getKeyFromUint8Array(
				chunk.slice(
					offset_,
					offset_ + this.#metaData.kvPairLength.total
				)
			);

			if (key === key_) {
				found = true;
				break;
			}
			if (key_ < key) {
				i = mid + 1;
			} else {
				j = mid;
			}
		}
		if (!found) return null;

		const offset_ = i * this.#metaData.kvPairLength.total;
		const line = chunk.slice(
			offset_,
			offset_ + this.#metaData.kvPairLength.total
		);
		return this.#convertUint8ArrayToDataNode(line, offset_);
	}

	#optiGetKeyBuffer(chunk: Uint8Array, offset: number) {
		// the key starts at offset+16;
		const getKeyBufferlength = getDataTypeByteLength(
			this.#options.keyDataType
		);
		const keyBuffer = new Uint8Array(getKeyBufferlength);

		// for (let i = offset + 16, j = 0; j < getKeyBufferlength; i++, j++) {
		// 	keyBuffer[j] = chunk[i];
		// }
		keyBuffer.set(
			chunk.slice(offset + 16, offset + 16 + getKeyBufferlength)
		);
		return DataTypeToValue(keyBuffer, this.#options.keyDataType);
	}

	async optreadKeyMmap(key: PossibleKeyType) {
		if (!this.#mmap) {
			return this.readKey(key);
		}

		const lb = this.#btree.nextLowerPair(key, this.#lowerPair);

		if (!lb || lb[1] === -1) return null;

		const chunk = new Uint8Array(
			Math.min(
				KVS_PER_PAGE * this.#metaData.kvPairLength.total,
				this.#fileSize - lb[1]
			)
		);
		this.#mmap!.copy(chunk, 0, lb[1], lb[1] + chunk.length);

		let i = lb[1],
			j = chunk.length / this.#metaData.kvPairLength.total;

		while (i < j) {
			const mid = i + Math.floor((j - i) / 2);
			const offset_ = mid * this.#metaData.kvPairLength.total;
			const keyBuffer_ = this.#optiGetKeyBuffer(chunk, offset_);

			if (keyBuffer_ === key) {
				return this.#convertUint8ArrayToDataNode(
					chunk.slice(
						offset_,
						offset_ + this.#metaData.kvPairLength.total
					),
					offset_
				);
			} else if (keyBuffer_ < key) {
				i = mid + 1;
			} else {
				j = mid;
			}
		}

		return null;
	}

	async readN(n: number) {
		const base = 3 + HEADER + METADATA;
		let offset: number;
		if (n < 0) {
			// read from the end
			n = Math.abs(n);
			offset = this.#fileSize - n * this.#metaData.kvPairLength.total;
		} else {
			offset = base + n * this.#metaData.kvPairLength.total;
		}
		return this.#readFromOffset(offset);
	}

	async readAll() {
		const data = [];
		const base = 3 + HEADER + METADATA;

		for (
			let i = base;
			i < this.#fileSize;
			i += this.#metaData.kvPairLength.total
		) {
			const data_ = await this.#readFromOffset(i);
			data.push(data_);
		}

		return data;
	}

	async write(data: Uint8Array[]) {
		await this.fileHandle.writev(data, 13);
		this.#fileSize = (await this.#fileHandle.stat()).size;
		// @ts-ignore
		let bestPageSize = Math.floor(this.#fileSize / mmap.PAGESIZE);
		// @ts-ignore
		if (this.#fileSize % mmap.PAGESIZE !== 0) {
			bestPageSize++;
		}
		// delete buffer
		this.#mmap = null;
		try {
			// @ts-ignore - this works but mmap typings are messed up
			this.#mmap = mmap.map(
				// @ts-ignore
				mmap.PAGESIZE * bestPageSize,
				// @ts-ignore
				mmap.PROT_READ,
				// @ts-ignore
				mmap.MAP_SHARED,
				this.#fileHandle.fd,
				0
			);
		} catch (e) {
			console.error(
				"Mmap creation failed with error:",
				e,
				"Switching to read from file"
			);
		}
		await this.#buildIndexTree(data, false, 13);
	}

	async clearData() {
		await this.#fileHandle.truncate(3 + HEADER + METADATA);
	}

	mayHasKey(key: PossibleKeyType) {
		return this.#bloomFilter.lookup(key.toString());
	}

	async hasKey(key: PossibleKeyType, useMmap: boolean) {
		const data = await (useMmap
			? this.optreadKeyMmap(key)
			: this.readKey(key));
		if (!data || data.build().delete) return false;
		return true;
	}

	async unlink() {
		this.#btree.clear();
		this.#bloomFilter.clear();

		// close the file handles
		this.#fileHandle.close();
		this.#bloomFileHandle.close();
		this.#indexFileHandle.close();

		// delete the files
		await fsp.unlink(this.#options.path);
		await fsp.unlink(`${this.#options.path.replace(".sst", "")}.bloom`);
		await fsp.unlink(`${this.#options.path.replace(".sst", "")}.index`);
	}

	// we will never use this function
	async append(data: Uint8Array[]) {
		// get current position of the cursor in the file
		const offset = this.#fileSize;
		await this.#fileHandle.writev(data, offset);

		// update the file size
		this.#fileSize = (await this.#fileHandle.stat()).size;

		// @ts-ignore
		let bestPageSize = Math.floor(this.#fileSize / mmap.PAGESIZE);
		// @ts-ignore
		if (this.#fileSize % mmap.PAGESIZE !== 0) {
			bestPageSize++;
		}
		// delete buffer
		this.#mmap = null;
		try {
			// @ts-ignore - this works but mmap typings are messed up
			this.#mmap = mmap.map(
				// @ts-ignore
				mmap.PAGESIZE * bestPageSize,
				// @ts-ignore
				mmap.PROT_READ,
				// @ts-ignore
				mmap.MAP_SHARED,
				this.#fileHandle.fd,
				0
			);
		} catch (e) {
			console.error(
				"Mmap creation failed with error:",
				e,
				"Switching to read from file"
			);
		}

		// update the index tree
		await this.#buildIndexTree(data, true, offset);
	}

	get options() {
		return this.#options;
	}

	get headerData() {
		return this.#headerData;
	}

	get metaData() {
		return this.#metaData;
	}

	get fileSize() {
		return this.#fileSize;
	}

	get btree() {
		return this.#btree;
	}

	get fileHandle() {
		return this.#fileHandle;
	}

	get btreeSize() {
		return this.#btree.size;
	}
}
