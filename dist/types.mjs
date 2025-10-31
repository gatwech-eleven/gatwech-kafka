class JsonSerializer {
  serialize(value) {
    return JSON.stringify(value);
  }
  deserialize(value) {
    return JSON.parse(value);
  }
}

export { JsonSerializer };
