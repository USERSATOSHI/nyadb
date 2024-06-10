export function mergeOptions<T>(defaultOptions: T, options: Partial<T>): T {
	return { ...defaultOptions, ...options };
}