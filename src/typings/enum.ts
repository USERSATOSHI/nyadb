// compression supported by nodejs
export enum CompressionFlag {
	None,
	Gzip,
	Brotli,
	Zlib,
}

export enum EncodingFlag {
	None,
	RunLengthEncoding,
	DictEncoding,
}

export enum SaveMode {
	DirectUpdate,
	TempReplace,
	Replicate,
}

export enum DatabaseMode {
	InMemory,
	Persistent,
}

export enum FileDataType {
	Str,
	I8,
	I16,
	I32,
	I64,
	U8,
	U16,
	U32,
	U64,
	F32,
	F64,
	Bool,
}

export enum WalMethod {
	Insert,
	Flush,
	Delete,
	Update,
}

export enum BloomFilterType {
	Classic,
	None,
}

export enum InMemBufferEvents {
	NeedsFlush = "needsFlush",
	BufferOpened = "bufferOpened",
}