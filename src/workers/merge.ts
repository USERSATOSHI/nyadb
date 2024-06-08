import { parentPort } from "worker_threads";
import DataNode from "../structs/Node.js";
import { sortAndMerge } from "../utils/sortAndMerge.js";
import { dataType } from "../typings/type.js";
import { IThreadedMergeAndSort } from "../typings/interface.js";



parentPort?.on( "message", async (data: IThreadedMergeAndSort) => {
	const { filePaths , dataSize , kvCount , growthFactor , options , level, pathForNextLevel } = data;

	const newSSTFilePath = await sortAndMerge(filePaths, dataSize, kvCount, growthFactor,pathForNextLevel, options, level);

	parentPort?.postMessage(newSSTFilePath);
	process.exit(0);
});