import { PriorityQueue } from "@js-sdsl/priority-queue";
import DataNode from "../structs/Node.js";
import { ISortAndMergeNode, ISSTFileOptions } from "../typings/interface.js";
import { OrderedMap } from "@js-sdsl/ordered-map";
import fsp from "node:fs/promises";
import SSTFile from "../files/SST.js";
import { dataType } from "../typings/type.js";
import { CompressionFlag, EncodingFlag } from "../typings/enum.js";

export function chunkify<T>(arr: T[], parts: number) {
	const chunks = [];
	const chunkSize = Math.ceil(arr.length / parts);
	for (let i = 0; i < arr.length; i += chunkSize) {
		chunks.push(arr.slice(i, i + chunkSize));
	}
	return chunks;
}

export const sortAndMerge = async (
	filePaths: string[],
	dataSize: number,
	kvCount: number,
	growthFactor: number,
	pathForNextLevel: string,
	options: {
		keyDataType: dataType;
		dataType: dataType;
		customHashFunction: ISSTFileOptions['customHashFunction']
	},
	level: number
) => {
	const resPaths = [];
	const pq: PriorityQueue<ISortAndMergeNode> = new PriorityQueue(
		undefined,
		(a, b) => {
			return a.data.key < b.data.key
				? -1
				: a.data.key > b.data.key
				? 1
				: 0;
		}
	);

	const fileHandles = await Promise.all(
		filePaths.map(async (path) => {
			const fileHandle = await fsp.open(path, "r");
			return fileHandle;
		})
	);

	const SortedArray = new Array<Uint8Array>(10000);

	const maxKvCount = kvCount * growthFactor;
	let currentKvCounter = 0;

	// add starting buffer to the priority queue
	let i = 0;
	for (const fileHandle of fileHandles) {
		const buffer = new Uint8Array(dataSize);
		await fileHandle.read(buffer, 0, dataSize, 13);

		const node: ISortAndMergeNode = {
			data: DataNode.fromUint8Array(buffer, 13, options),
			index: 0,
			arr: i++,
		};

		pq.push(node);
	}

	let newSSFile = new SSTFile({
		path: `${pathForNextLevel}/${Date.now()}.sst`,
		dataType: options.dataType,
		keyDataType: options.keyDataType,
		kvCount: maxKvCount,
		kvPerPage: 1000,
		doBatchValidation: false,
		encoding: EncodingFlag.None,
		compression: CompressionFlag.None,
		customHashFunction: options.customHashFunction
	});
	resPaths.push(newSSFile.options.path);
	await newSSFile.open();

	let idx = 0;
	let LastNode: DataNode | null = null;

	while (!pq.empty()) {
		const node = pq.pop();

		if (!node) break;
		const offsetForNext = node.data.offset + dataSize;
		if (LastNode === null) {
			LastNode = node.data;
		} else {
			if (LastNode.key === node.data.key) {
				if (LastNode.timestamp < node.data.timestamp) {
					LastNode = node.data;
					SortedArray[idx] = LastNode.toUint8Array();
				}
			} else {
				SortedArray[idx++] = LastNode.toUint8Array();
				LastNode = node.data;
			}
		}

		const fileHandle = fileHandles[node.arr];
		const buffer = new Uint8Array(dataSize);
		const { bytesRead } = await fileHandle.read(
			buffer,
			0,
			dataSize,
			offsetForNext
		);
		if (bytesRead === 0) {
			continue;
		}

		const newNode: ISortAndMergeNode = {
			data: DataNode.fromUint8Array(buffer, offsetForNext, options),
			index: node.index + 1,
			arr: node.arr,
		};

		pq.push(newNode);

		if (idx === 10000) {
			await newSSFile.append(SortedArray);
			currentKvCounter += 10000;

			if (currentKvCounter >= maxKvCount) {
				await newSSFile.close();
				// create new SST file
				newSSFile = new SSTFile({
					path: `${pathForNextLevel}/${Date.now()}.sst`,
					dataType: options.dataType,
					keyDataType: options.keyDataType,
					kvCount: maxKvCount,
					kvPerPage: 1000,
					doBatchValidation: false,
					encoding: EncodingFlag.None,
					compression: CompressionFlag.None,
					customHashFunction: options.customHashFunction
				});
				await newSSFile.open();
				resPaths.push(newSSFile.options.path);
				currentKvCounter = 0;
			}
			idx = 0;
		}
	}

	if (idx !== 0) {
		const allowedSizeLeft = maxKvCount - currentKvCounter;
		const lastSliceLength = Math.min(allowedSizeLeft, idx);

		await newSSFile.append(SortedArray.slice(0, lastSliceLength));

		const left = idx - lastSliceLength;

		if (left !== 0) {
			await newSSFile.close();

			// create new SST file
			newSSFile = new SSTFile({
				path: `${pathForNextLevel}/${Date.now()}.sst`,
				dataType: options.dataType,
				keyDataType: options.keyDataType,
				kvCount: maxKvCount,
				kvPerPage: 1000,
				doBatchValidation: false,
				encoding: EncodingFlag.None,
				compression: CompressionFlag.None,
				customHashFunction: options.customHashFunction
			});
			await newSSFile.open();
			resPaths.push(newSSFile.options.path);

			await newSSFile.append(
				SortedArray.slice(lastSliceLength, lastSliceLength + left)
			);
		}
	}

	await newSSFile.close();
	await Promise.all(fileHandles.map((fh) => fh.close()));

	return resPaths;
};
