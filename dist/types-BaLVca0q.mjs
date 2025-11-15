//#region src/types/kafka.ts
var JsonSerializer = class {
	serialize(value) {
		return JSON.stringify(value);
	}
	deserialize(value) {
		return JSON.parse(value);
	}
};

//#endregion
export { JsonSerializer as t };