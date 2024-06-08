import Column from "../structs/Column.js";
import DataNode from "../structs/Node.js";
import {
	BloomFilterType,
	CompressionFlag,
	EncodingFlag,
	InMemBufferEvent,
} from "./enum.js";
import { dataType, PossibleKeyType } from "./type.js";

export interface ISSTFileOptions {
	path: string;
	dataType: dataType;
	keyDataType: dataType;
	encoding: EncodingFlag;
	compression: CompressionFlag;
	kvCount: number;
	doBatchValidation: boolean;
	kvPerPage: number;
}

export interface IWalFileOptions {
	path: string;
	maxSize: number;
	maxBufferSize: number;
}

export interface IHeaderData {
	magicNumber: `0x${string}`;
	versionFlag: number;
	compressionFlag: CompressionFlag;
	encodingFlag: EncodingFlag;
}

export interface IMetadata {
	valueDataType: dataType;
	keyDataType: dataType;
	kvPairLength: {
		key: number;
		value: number;
		checksum: number;
		total: number;
	};
}

export interface IDataNodeOptions {
	key: PossibleKeyType;
	value: unknown;
	keyType: dataType;
	valueType: dataType;
	offset: number;
	delete: boolean;
	checksum: Uint8Array;
	length: number;
	timestamp: number;
	dataBuffer: Uint8Array;
}

export interface IInMemoryBufferOptions {
	threshHold: number;
}

export interface ISSTMangerOptions {
	path: string;
	sstThreshold: number;
	valueType: dataType;
	threadsForMerge: number;
	keyType: dataType;
	levels: number;
	growthFactor: number;
	sstConfig: Omit<
		ISSTFileOptions,
		"path" | "max" | "min" | "dataType" | "KeyDataType"
	>;
	readMmap: boolean;
}

export interface IColumnOptions {
	name: string;
	valueType: dataType;
	keyType: dataType;
	sstConfig: ISSTMangerOptions;
	memBufferConfig: IInMemoryBufferOptions;
	walConfig: IWalFileOptions;
	cacheSize: number;
}

export interface ILargeSSTFileOptions extends ISSTFileOptions {
	bloomFilter: BloomFilterType;
}

export interface IInMemBufferEvents {
	[InMemBufferEvent.NeedsFlush]: () => void;
	[InMemBufferEvent.BufferOpened]: () => void;
}

export interface ISortAndMergeNode {
	data: DataNode;
	index: number;
	arr: number;
}

export interface IThreadedMergeAndSort {
	filePaths: string[];
	dataSize: number;
	kvCount: number;
	growthFactor: number;
	pathForNextLevel: string;
	options: {
		keyDataType: dataType;
		dataType: dataType;
	};
	level: number;
}

export interface IThreadedReadAll { path: string, dataSize:number }

export interface ITableOptions {
	name: string;
	columns: Column[];
}

export interface IDatabaseOptions {
	path: string;
	name: string;
	tables: ITableOptions[];
}