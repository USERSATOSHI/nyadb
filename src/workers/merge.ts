import { parentPort } from "worker_threads";
import { sortAndMerge } from "../utils/sortAndMerge.js";
import { dataType } from "../typings/type.js";
import {
	ISSTFileOptions,
	IThreadedMergeAndSort,
} from "../typings/interface.js";

parentPort?.on("message", async (data: IThreadedMergeAndSort) => {
	const {
		filePaths,
		dataSize,
		kvCount,
		growthFactor,
		options,
		level,
		pathForNextLevel,
	} = data;

	const newOptions: {
		keyDataType: dataType;
		dataType: dataType;
		customHashFunction: ISSTFileOptions["customHashFunction"];
	} = {
		keyDataType: options.keyDataType as dataType,
		dataType: options.dataType as dataType,
		customHashFunction: new Function(
			`return ${options.customHashFunction}`
		)(),
	};

	const newSSTFilePath = await sortAndMerge(
		filePaths,
		dataSize,
		kvCount,
		growthFactor,
		pathForNextLevel,
		newOptions,
		level
	);

	parentPort?.postMessage(newSSTFilePath);
	process.exit(0);
});
