//#region src/types/index.d.ts
interface InitOptions {
  name?: string;
  brokers?: string;
  docker?: boolean;
}
interface CreateTopicsOptions {
  config?: string;
  dryRun?: boolean;
}
interface ListTopicsOptions {
  filter?: string;
  json?: boolean;
}
interface LagOptions {
  topic?: string;
  watch?: boolean;
}
interface HealthOptions {
  json?: boolean;
}
interface GenerateTypesOptions {
  config?: string;
  output?: string;
}
//#endregion
export { CreateTopicsOptions, GenerateTypesOptions, HealthOptions, InitOptions, LagOptions, ListTopicsOptions };