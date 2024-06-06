import { parentPort } from "node:worker_threads";
import DataNode from "../structs/Node.js";
import { dataType, PossibleKeyType } from "../typings/type.js";
import { OrderedMap } from "@js-sdsl/ordered-map";

// here we get the merge data from the worker

parentPort?.on(
	"message",
	async (data: {
		data: Uint8Array[];
		options: { keyDataType: dataType; dataType: dataType };
		files: string[];
	}) => {
		// convert uint8array[] to DataNode[];
		let offset = 0;
		const nodes = data.data.map((node) => {
			const dnode = DataNode.fromUint8Array(node, offset, data.options);
			offset += node.length;
			return dnode;
		});

		// merge the data and send it back to the worker
		const btree = new OrderedMap<PossibleKeyType, DataNode>();
		for (const node of nodes) {
			if (node.delete) {
				btree.eraseElementByKey(node.key);
			} else {
				if (
					(btree.getElementByKey(node.key)?.timestamp || 0n) <
					node.timestamp
				)
					btree.setElement(node.key, node);
			}
		}

		const mergedData = [];
		for (const [key, value] of btree) {
			mergedData.push(value.toUint8Array());
		}

		parentPort?.postMessage({
			data: mergedData,
			files: data.files,
		});
	}
);
