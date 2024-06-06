import { BloomFilterType, CompressionFlag, EncodingFlag } from "./enum.js";
import { dataType, PossibleKeyType } from "./type.js";

export interface ISSTFileOptions {
	path: string;
	dataType: dataType;
	keyDataType: dataType;
	encoding: EncodingFlag;
	compression: CompressionFlag;
	kvCount: number;
	doBatchValidation: boolean;
	kvPerPage:number;
}

export interface IWalFileOptions {
	path:string;
	maxSize: number;
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
	}
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
	timestamp: bigint;
}

export interface IInMemoryBufferOptions {
	threshHold: number;
}

export interface ISSTMangerOptions {
	path: string;
	sstThreshold: number;
	valueType: dataType;
	keyType: dataType;
	levels: number;
	growthFactor: number;
	sstConfig: Omit<ISSTFileOptions , 'path' | 'max' | 'min' | 'dataType' | 'KeyDataType'>;
	readMmap: boolean;
}

export interface IColumnOptions {
	name: string;
	valueType: dataType;
	keyType: dataType;
	sstConfig: ISSTMangerOptions;
	memBufferConfig: IInMemoryBufferOptions;
  walConfig: IWalFileOptions;	
  debug?: boolean;
}

export interface ILargeSSTFileOptions extends ISSTFileOptions {
	bloomFilter: BloomFilterType;
	
}