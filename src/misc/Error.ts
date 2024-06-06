export default class NyaDBError {
	static InitError =  class InitError extends Error {
		constructor(message: string, method:string, class_: object) {
			super(`[${class_.constructor.name}#${method}]: ${message}`);
			this.name = "NyaDBError#Init";
		}
	}
}