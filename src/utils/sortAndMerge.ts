import { PriorityQueue } from "@js-sdsl/priority-queue";
import DataNode from "../structs/Node.js";
import { ISortAndMergeNode } from "../typings/interface.js";
import { OrderedMap } from "@js-sdsl/ordered-map";

export const chunkify = (arr: DataNode["data"][][], parts: number) => {
	const chunks = [];
	const chunkSize = Math.ceil(arr.length / parts);
	for (let i = 0; i < arr.length; i += chunkSize) {
		chunks.push(arr.slice(i, i + chunkSize));
	}
	return chunks;
};

export const sortAndMerge = (
	data: DataNode["data"][][],
	uint8Array = false
): DataNode["data"][] | Uint8Array[] => {
	const pq = new PriorityQueue<ISortAndMergeNode>(undefined, (a, b) =>
		a.data.key > b.data.key ? 1 : -1
	);
	for (let i = 0; i < data.length; i++) {
		pq.push({ data: data[i][0], index: 0, arr: i });
	}
	const res = new OrderedMap<DataNode["key"], DataNode>();
	for (let i = 0; i < data.length * data[0].length; i++) {
		const node = pq.pop();

		if (!node) break;

		const resNode = res.getElementByKey(node.data.key);
		if (resNode && resNode.data.timestamp < node.data.timestamp) {
			res.setElement(node.data.key, new DataNode(node.data));
		} else if (!resNode) { 
			res.setElement(node.data.key, new DataNode(node.data));
		}

		if (node.index + 1 < data[node.arr].length) {
			pq.push({
				data: data[node.arr][node.index + 1],
				index: node.index + 1,
				arr: node.arr,
			});
		}
	}

	const finalRes = [];
	for (const [_, value] of res) {
		if (uint8Array) {
			finalRes.push(value.toUint8Array());
		} else {
			finalRes.push(value.data);
		}
	}
	// @ts-ignore
	return finalRes;
};
