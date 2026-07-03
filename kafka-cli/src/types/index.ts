export interface InitOptions {
  name?: string;
  brokers?: string;
  docker?: boolean;
}

export interface CreateTopicsOptions {
  config?: string;
  dryRun?: boolean;
}

export interface ListTopicsOptions {
  filter?: string;
  json?: boolean;
}

export interface LagOptions {
  topic?: string;
  watch?: boolean;
}

export interface HealthOptions {
  json?: boolean;
}

export interface GenerateTypesOptions {
  config?: string;
  output?: string;
}
