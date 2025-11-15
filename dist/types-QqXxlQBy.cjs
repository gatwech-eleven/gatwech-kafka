
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
Object.defineProperty(exports, 'JsonSerializer', {
  enumerable: true,
  get: function () {
    return JsonSerializer;
  }
});