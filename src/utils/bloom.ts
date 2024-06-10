export function getCellAndHashCount(
	keysCount: number,
	errorRate: number
): [number, number] {
	//ceil((n * log(p)) / log(1 / pow(2, log(2))));
	const bitCount = Math.ceil((keysCount * Math.log(errorRate)) / Math.log(1 / Math.pow(2, Math.log(2)))
	);
	const hashFunctionCount = Math.round((bitCount / keysCount) * Math.LN2);

	return [bitCount, hashFunctionCount];
}
