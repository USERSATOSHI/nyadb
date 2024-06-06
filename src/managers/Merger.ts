import { parentPort } from "node:worker_threads";
import DataNode from "../structs/Node.js";
import { dataType, PossibleKeyType } from "../typings/type.js";
import { OrderedMap } from "@js-sdsl/ordered-map";
import { PriorityQueue } from "@js-sdsl/priority-queue";
import { sortAndMerge } from "../utils/sortAndMerge.js";

// here we get the merge data from the worker

parentPort?.on(
	"message",
	async (data: {
		data: DataNode["data"][][];
		files: string[];
		level: number;
	}) => {
		const mergedData = sortAndMerge(data.data);

		parentPort?.postMessage({
			data: mergedData,
			files: data.files,
			level: data.level,
		});
	}
);
