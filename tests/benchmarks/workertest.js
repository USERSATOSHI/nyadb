import { Worker, isMainThread, parentPort } from "worker_threads";
import { PriorityQueue } from "@js-sdsl/priority-queue";
import { cpus } from "os";

// Function to split an array into a given number of parts
const chunkify = (arr, parts) => {
    const chunks = [];
    const chunkSize = Math.ceil(arr.length / parts);
    for (let i = 0; i < arr.length; i += chunkSize) {
        chunks.push(arr.slice(i, i + chunkSize));
    }
    return chunks;
};

if (isMainThread) {
    const data = Array.from({ length: 10 }, () => 
        Array.from({ length: 1000000 }, () => Math.floor(Math.random() * 100000)).sort((a, b) => a - b)
    );

    const numThreads = 1;
    const chunks = chunkify(data, numThreads);
    const sortedPartials = [];
	const workers = [];
	for (let i = 0; i < numThreads; i++) {
		workers.push(new Worker(import.meta.filename));
	}
	const start = performance.now();
    for (let i = 0; i < chunks.length; i++) {
       const worker = workers[i];
        worker.postMessage(chunks[i]);
        worker.on("message", (msg) => {
            sortedPartials.push(msg);
            if (sortedPartials.length === chunks.length) {
                // Merging the sorted partial results
				for(const worker of workers) {
					worker.terminate();
				}
                const pq = new PriorityQueue(undefined,(a, b) => a.data - b.data);
                for (let i = 0; i < sortedPartials.length; i++) {
                    pq.push({ data: sortedPartials[i][0], index: 0, arr: i });
                }
                const res = [];
                for (let i = 0; i < sortedPartials.length * sortedPartials[0].length; i++) {
                    const node = pq.pop();
                    if (!node) break;
                    res.push(node.data);
                    if (node.index + 1 < sortedPartials[node.arr].length) {
                        pq.push({
                            data: sortedPartials[node.arr][node.index + 1],
                            index: node.index + 1,
                            arr: node.arr,
                        });
                    }
                }
                console.log(performance.now() - start);
                console.log(res);
            }
        });
    }
} else {
    parentPort.on("message", (data) => {
        const pq = new PriorityQueue(undefined,(a, b) => a.data - b.data);
        for (let i = 0; i < data.length; i++) {
            pq.push({ data: data[i][0], index: 0, arr: i });
        }
        const res = [];
        for (let i = 0; i < data.length * data[0].length; i++) {
            const node = pq.pop();
            res.push(node.data);
            if (node.index + 1 < data[node.arr].length) {
                pq.push({
                    data: data[node.arr][node.index + 1],
                    index: node.index + 1,
                    arr: node.arr,
                });
            }
        }
        parentPort.postMessage(res);
		process.exit();
    });
}
