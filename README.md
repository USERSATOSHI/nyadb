# NyaDB

- a KV store based on LSM Tree implemented in Javascript.

## Features

> - **LSM Tree**: NyaDB is a key-value store based on LSM Tree.
> - **Performance**: NyaDB is designed to be fast and efficient.
> - **MMAP**: NyaDB uses MMAP to memory map the data file for fast reads and can fallback to file I/O if MMAP is not available.
> - **Snapshot**: NyaDB supports snapshots for backup and restore.
> - **Blooms Filter**: NyaDB uses bloom filters to reduce disk reads.
> - **Background Compaction**: NyaDB uses worker threads to merge and compact SSTables to keep the database size in check. and shouldnt use much memory.
> - **Write Ahead Log**: NyaDB uses Write Ahead Log to ensure durability.

## Installation

```bash
npm install https://github.com/usersatoshi/nyadb
```

or

```bash
git clone https://github.com/usersatoshi/nyadb
```

## Usage

```javascript
import { Column, Database } from "nyadb";

const money = new Column({
 name: "money",
 keyType: "u32",
 valueType: "u32",
 cacheSize: 10000,
});

const db = new Database({
 name: "test",
 path: "./b/database",
 tables: [
  {
   name: "main",
   columns: [money],
  },
 ],
});

await db.init();

await db.insert("main", {
 column: "money",
 key: 1,
 value: 100,
});

const value = await db.get("main", {
 column: "money",
 key: 1,
});

console.log(value); // DataNode {} ( use .value to get the value )

console.log(
 await db.has("main", {
  column: "money",
  key: 1,
 })
); // true

await db.close();
```

## API Documentation

[Docs](https://usersatoshi.github.io/nyadb/)

## Inspiration

> - I made this as more of a challenge with someone to see who could make a better database. I don't know if I won or not, but I'm happy with the result.
> - Certain decisions on why some code are written in a certain way is because they worked faster , so some part of the code might look a bit weird.
> - I'm not a database expert, so I'm sure there are a lot of things that could be improved.
> - I'm open to suggestions :)

## Things I Would Do

> - I dont like the current WAL implementation, the Readable is a bit weird and can be come out of sync with memtable ( which shouldnt be an issue if u are not spamming writes )
> - range queries.
> - transactions Support.
> - Find functions such as findOne , findMany , all etc.
> - Better error handling.

## Performance

> ```bash
> $ node ./tests/benchmarks/databasebench.js
> ```
>
> Environment:
>
> - Arch Linux 6.9.3-arch1
> - 11th Gen Intel(R) Core(TM) i5-1135G7 (8) @ 4.20 GHz
> - 16GB RAM DDR4
> - Node v20.14.0
> - NyaDB v1.0.0

---

> Results:
>
> - ***for keyType: u32 and valueType: u32***
>   | Operation | ops/s | StdDev |
>   | --------- | ----- | ------ |
>   |Insert|221,883 ops/s | +/-15.75%|
>   | Get|    903,891 ops/s|  +/-1.29%|
>   | Has|    1,031,078 ops/s | +/-0.83%|
>   | BloomCheck| 3,335,837 ops/s | +/-0.94%|
>
> - ***for keyType: str:18 and valueType: u32***
>
>   | Operation | ops/s | StdDev |
>   | --------- | ----- | ------ |
>   | Insert| 150,064 ops/s | +/-20.90%|
>   | Get| 122,152 ops/s | +/-61.07%|
>   | Has| 309,502 ops/s |+/-2.71%|
>   | BloomCheck| 479,791 ops/s | +/-0.91%|
>
> - ***for keyType: u64 and valueType: u32***
>
>   | Operation | ops/s | StdDev |
>   | --------- | ----- | ------ |
>   | Insert|  200,914 ops/s | +/-14.38%|
>   | Get|  599,735 ops/s | +/-21.83%|
>   | Has|  835,765 ops/s | +/-0.75%|
>   | BloomCheck|  2,027,288 ops/s | +/-0.71%|

## Technical Details

> - **File Format**:
>
> ```javascript
>    Header Length (1 bytes)       
> | Header                        |
> | +---------------------------+ |
> | | Magic Number (4 bytes)    | |
> | | Version Flag (1 byte)     | |
> | | Compression Flag (1 byte) | |
> | | Encoding Flag (1 byte)    | |
> | +---------------------------+ |
> Metadata Length (1 bytes)
> | Metadata                     |
> | +---------------------------+ |
> | | Value Data Type (1 byte) | |
> | | Key Data Type (1 byte)   | |
> | | KVPair Length (1 byte)   | |
> | +---------------------------+ |
> | | KV Pair (Amount -> KvCount) ||
> ```
>
> - **Data Format**:
>
> ```javascript
> STARTDELIMITER (4 bytes ) | key length (4 bytes) | value length (4 bytes) | checksum length (4 bytes) | key (key length) | value (value length) | checksum (4 bytes ) | timestamp (8 bytes) | deleted (1 byte) | ENDDELIMITER (4 bytes) | (repeat for KVS_PER_PAGE times)
> ```
>
> - **LSM Tree**:
>   - **Memtable**: In-memory table that stores the most recent data, it uses a OrderedMap to store data sorted by key.
>   - **SSTable**: Immutable on-disk table that stores data in sorted order, it uses a B+Tree to Index the data and a Bloom Filter to reduce disk reads.
>     - **Bloom Filter**: A probabilistic data structure that tells us if a key exists in the SSTable. this uses a bit array and multiple hash functions according to the datatype.
>   - **Index**: A B+Tree that stores the key and the offset of the data in the SSTable.
> - **Compaction**: Merging and compacting SSTables to keep the database size in check. this is done via worker threads and k way merge algorithm using a priority queue and file pointers to minimize memory usage.
>
> - **Write Ahead Log**: A log that stores all the writes to the database to ensure durability. Used Readable Stream to buffer the data and piped it to WriteStream to write to the file , this respected the backpressure and doesn't use much memory.
>
> - **MMAP**: Memory-mapped file that maps the data file to memory for fast reads. if MMAP is not available, it falls back to file I/O. this is done via @raygun-nickj/mmap-io fork of mmap-io.
>
> - **Snapshot**: A snapshot of the database that can be used for backup and restore. this is done by creating a tar of the data file and the WAL file to .snapshots folder.
>
> - **Cache**: A LRU cache that stores the most recent data to reduce disk reads. this is done via LruMap from mnemonist.
