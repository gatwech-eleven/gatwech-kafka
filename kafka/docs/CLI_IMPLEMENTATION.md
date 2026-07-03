# @gatwech/kafka-cli Implementation Plan

## Overview

A command-line tool for managing Kafka topics, checking health, generating configs, and improving developer workflow.

## Project Structure

```
packages/kafka-cli/
├── src/
│   ├── commands/
│   │   ├── init.ts
│   │   ├── topics/
│   │   │   ├── create.ts
│   │   │   ├── list.ts
│   │   │   ├── describe.ts
│   │   │   └── delete.ts
│   │   ├── consumer/
│   │   │   ├── lag.ts
│   │   │   └── groups.ts
│   │   ├── health.ts
│   │   └── generate/
│   │       ├── types.ts
│   │       └── config.ts
│   ├── utils/
│   │   ├── kafka-client.ts
│   │   ├── logger.ts
│   │   ├── spinner.ts
│   │   └── templates.ts
│   ├── types/
│   │   └── index.ts
│   ├── cli.ts
│   └── index.ts
├── templates/
│   ├── kafka.config.ts.template
│   ├── topics.ts.template
│   ├── .env.template
│   └── docker-compose.yaml.template
├── package.json
├── tsconfig.json
└── README.md
```

## Dependencies

```json
{
  "dependencies": {
    "@gatwech/kafka": "workspace:*",
    "commander": "^12.0.0",
    "inquirer": "^9.2.0",
    "chalk": "^5.3.0",
    "ora": "^8.0.0",
    "cli-table3": "^0.6.3",
    "dotenv": "^16.4.0",
    "zod": "^4.1.5"
  },
  "devDependencies": {
    "@types/inquirer": "^9.0.0",
    "@types/node": "^20.0.0",
    "typescript": "^5.0.0",
    "vitest": "^1.0.0"
  }
}
```

## package.json Configuration

```json
{
  "name": "@gatwech/kafka-cli",
  "version": "0.1.0",
  "description": "CLI tool for @gatwech/kafka",
  "bin": {
    "kafka-cli": "./dist/index.js"
  },
  "files": [
    "dist",
    "templates"
  ],
  "scripts": {
    "build": "tsc",
    "dev": "tsx src/index.ts",
    "test": "vitest"
  }
}
```

## Command Specifications

### 1. `kafka-cli init`

**Purpose:** Initialize a new Kafka project with config files

**Usage:**
```bash
kafka-cli init [options]
```

**Options:**
- `--name <name>` - Project name
- `--brokers <brokers>` - Kafka brokers (comma-separated)
- `--typescript` - Generate TypeScript files (default: true)
- `--docker` - Include docker-compose.yaml

**Implementation:**

```typescript
// src/commands/init.ts
import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import fs from 'fs/promises';
import path from 'path';

export async function initCommand(options: InitOptions) {
  console.log(chalk.blue.bold('\n🚀 Initializing Kafka project...\n'));
  
  // Prompt for missing options
  const answers = await inquirer.prompt([
    {
      type: 'input',
      name: 'name',
      message: 'Project name:',
      default: options.name || 'my-kafka-app',
      when: !options.name,
    },
    {
      type: 'input',
      name: 'brokers',
      message: 'Kafka brokers (comma-separated):',
      default: 'localhost:9092',
      when: !options.brokers,
    },
    {
      type: 'confirm',
      name: 'docker',
      message: 'Include docker-compose.yaml?',
      default: true,
      when: options.docker === undefined,
    },
  ]);
  
  const config = { ...options, ...answers };
  const spinner = ora('Creating files...').start();
  
  try {
    // Create directory structure
    await fs.mkdir('src', { recursive: true });
    
    // Generate kafka.config.ts
    await generateKafkaConfig(config);
    
    // Generate topics.ts
    await generateTopicsFile(config);
    
    // Generate .env
    await generateEnvFile(config);
    
    // Generate docker-compose.yaml (if requested)
    if (config.docker) {
      await generateDockerCompose(config);
    }
    
    spinner.succeed('Files created successfully!');
    
    console.log(chalk.green('\n✅ Project initialized!\n'));
    console.log(chalk.gray('Next steps:'));
    console.log(chalk.gray('  1. Update src/topics.ts with your topic definitions'));
    console.log(chalk.gray('  2. Run: kafka-cli topics create'));
    console.log(chalk.gray('  3. Start building!\n'));
  } catch (error) {
    spinner.fail('Failed to create files');
    throw error;
  }
}

async function generateKafkaConfig(config: InitOptions) {
  const template = await fs.readFile(
    path.join(__dirname, '../../templates/kafka.config.ts.template'),
    'utf-8'
  );
  
  const content = template
    .replace('{{PROJECT_NAME}}', config.name)
    .replace('{{BROKERS}}', config.brokers);
  
  await fs.writeFile('src/kafka.config.ts', content);
}
```

**Templates:**

```typescript
// templates/kafka.config.ts.template
import { createKafkaClient } from '@gatwech/kafka';

export const kafka = createKafkaClient({
  config: {
    clientId: '{{PROJECT_NAME}}',
    brokers: process.env.KAFKA_BROKERS?.split(',') || ['{{BROKERS}}'],
    resilience: {
      enabled: true,
      circuitBreaker: {
        enabled: true,
        failureThreshold: 5,
        resetTimeoutMs: 30000,
      },
      reconnection: {
        enabled: true,
        intervalMs: 60000,
      },
    },
    autoConnectAsync: true,
  },
});

export async function initKafka() {
  // Kafka connects in background
  return true;
}

export async function shutdownKafka() {
  await kafka.disconnect();
}
```

### 2. `kafka-cli topics create`

**Purpose:** Create topics from configuration file

**Usage:**
```bash
kafka-cli topics create [options]
```

**Options:**
- `--config <path>` - Path to topics config file (default: src/topics.ts)
- `--dry-run` - Show what would be created without creating

**Implementation:**

```typescript
// src/commands/topics/create.ts
import chalk from 'chalk';
import ora from 'ora';
import { createKafkaClient } from '@gatwech/kafka';
import { loadTopicsConfig } from '../../utils/config-loader';

export async function createTopicsCommand(options: CreateTopicsOptions) {
  const spinner = ora('Loading topics configuration...').start();
  
  try {
    const topics = await loadTopicsConfig(options.config);
    spinner.succeed(`Loaded ${topics.length} topic(s)`);
    
    if (options.dryRun) {
      console.log(chalk.yellow('\n🔍 Dry run - no topics will be created\n'));
      topics.forEach(topic => {
        console.log(chalk.cyan(`  ${topic.name}`));
        console.log(chalk.gray(`    Partitions: ${topic.partitions}`));
        console.log(chalk.gray(`    Replication: ${topic.replicationFactor}\n`));
      });
      return;
    }
    
    const client = await createClient();
    spinner.start('Creating topics...');
    
    await client.provisionTopics(topics);
    
    spinner.succeed('Topics created successfully!');
    
    console.log(chalk.green('\n✅ All topics created\n'));
  } catch (error) {
    spinner.fail('Failed to create topics');
    throw error;
  }
}
```

### 3. `kafka-cli topics list`

**Purpose:** List all Kafka topics

**Usage:**
```bash
kafka-cli topics list [options]
```

**Options:**
- `--filter <pattern>` - Filter topics by pattern
- `--json` - Output as JSON

**Implementation:**

```typescript
// src/commands/topics/list.ts
import chalk from 'chalk';
import Table from 'cli-table3';
import { createKafkaClient } from '@gatwech/kafka';

export async function listTopicsCommand(options: ListTopicsOptions) {
  const client = await createClient();
  const admin = client.admin();
  
  await admin.connect();
  
  try {
    const topics = await admin.listTopics();
    const filtered = options.filter
      ? topics.filter(t => t.match(new RegExp(options.filter)))
      : topics;
    
    if (options.json) {
      console.log(JSON.stringify(filtered, null, 2));
      return;
    }
    
    const table = new Table({
      head: [chalk.cyan('Topic'), chalk.cyan('Partitions'), chalk.cyan('Replicas')],
    });
    
    for (const topic of filtered) {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const partitions = metadata.topics[0].partitions.length;
      const replicas = metadata.topics[0].partitions[0].replicas.length;
      
      table.push([topic, partitions, replicas]);
    }
    
    console.log(table.toString());
    console.log(chalk.gray(`\nTotal: ${filtered.length} topic(s)\n`));
  } finally {
    await admin.disconnect();
  }
}
```

### 4. `kafka-cli consumer lag`

**Purpose:** Check consumer group lag

**Usage:**
```bash
kafka-cli consumer lag <group-id> [options]
```

**Options:**
- `--topic <topic>` - Filter by specific topic
- `--watch` - Continuously watch lag

**Implementation:**

```typescript
// src/commands/consumer/lag.ts
import chalk from 'chalk';
import Table from 'cli-table3';
import { createKafkaClient } from '@gatwech/kafka';

export async function consumerLagCommand(groupId: string, options: LagOptions) {
  const client = await createClient();
  const admin = client.admin();
  
  await admin.connect();
  
  const displayLag = async () => {
    const offsets = await admin.fetchOffsets({ groupId });
    
    const table = new Table({
      head: [
        chalk.cyan('Topic'),
        chalk.cyan('Partition'),
        chalk.cyan('Current'),
        chalk.cyan('High Water'),
        chalk.cyan('Lag'),
      ],
    });
    
    for (const topic of offsets) {
      for (const partition of topic.partitions) {
        const lag = partition.high - partition.offset;
        const lagColor = lag > 1000 ? chalk.red : lag > 100 ? chalk.yellow : chalk.green;
        
        table.push([
          topic.topic,
          partition.partition,
          partition.offset,
          partition.high,
          lagColor(lag.toString()),
        ]);
      }
    }
    
    console.clear();
    console.log(table.toString());
    console.log(chalk.gray(`\nLast updated: ${new Date().toLocaleTimeString()}\n`));
  };
  
  if (options.watch) {
    setInterval(displayLag, 5000);
  }
  
  await displayLag();
  
  if (!options.watch) {
    await admin.disconnect();
  }
}
```

### 5. `kafka-cli health`

**Purpose:** Check Kafka cluster health

**Usage:**
```bash
kafka-cli health [options]
```

**Options:**
- `--json` - Output as JSON

**Implementation:**

```typescript
// src/commands/health.ts
import chalk from 'chalk';
import { createKafkaClient } from '@gatwech/kafka';

export async function healthCommand(options: HealthOptions) {
  const client = await createClient();
  
  try {
    await client.connect();
    const health = client.getHealthStatus();
    const connection = client.getConnectionStatus();
    
    if (options.json) {
      console.log(JSON.stringify({ health, connection }, null, 2));
      return;
    }
    
    console.log(chalk.bold('\n📊 Kafka Health Status\n'));
    
    const statusColor = health.status === 'healthy' ? chalk.green : chalk.yellow;
    console.log(`Status: ${statusColor(health.status)}`);
    console.log(`Connected: ${connection.connected ? chalk.green('✓') : chalk.red('✗')}`);
    console.log(`Uptime: ${health.details.uptime}ms`);
    console.log(`Connection Attempts: ${connection.connectionAttempts}`);
    
    if (connection.lastError) {
      console.log(chalk.red(`Last Error: ${connection.lastError}`));
    }
    
    console.log();
  } finally {
    await client.disconnect();
  }
}
```

### 6. `kafka-cli generate:types`

**Purpose:** Generate TypeScript types from topic schemas

**Usage:**
```bash
kafka-cli generate:types [options]
```

**Options:**
- `--config <path>` - Path to topics config
- `--output <path>` - Output file path (default: src/topics.generated.ts)

**Implementation:**

```typescript
// src/commands/generate/types.ts
import chalk from 'chalk';
import ora from 'ora';
import fs from 'fs/promises';
import { loadTopicsConfig } from '../../utils/config-loader';

export async function generateTypesCommand(options: GenerateTypesOptions) {
  const spinner = ora('Loading topics configuration...').start();
  
  const topics = await loadTopicsConfig(options.config);
  spinner.succeed(`Loaded ${topics.length} topic(s)`);
  
  spinner.start('Generating types...');
  
  const types = generateTypeDefinitions(topics);
  
  await fs.writeFile(options.output, types);
  
  spinner.succeed(`Types generated: ${options.output}`);
  console.log(chalk.green('\n✅ TypeScript types generated!\n'));
}

function generateTypeDefinitions(topics: Topic[]): string {
  const imports = `import { z } from 'zod';\n\n`;
  
  const typeMap = topics.map(topic => {
    return `  '${topic.name}': z.infer<typeof ${topic.schemaName}>;`;
  }).join('\n');
  
  return `${imports}export type Topics = {\n${typeMap}\n};\n`;
}
```

## CLI Entry Point

```typescript
// src/cli.ts
import { Command } from 'commander';
import chalk from 'chalk';
import { initCommand } from './commands/init';
import { createTopicsCommand } from './commands/topics/create';
import { listTopicsCommand } from './commands/topics/list';
import { consumerLagCommand } from './commands/consumer/lag';
import { healthCommand } from './commands/health';
import { generateTypesCommand } from './commands/generate/types';

const program = new Command();

program
  .name('kafka-cli')
  .description('CLI tool for @gatwech/kafka')
  .version('0.1.0');

// Init command
program
  .command('init')
  .description('Initialize a new Kafka project')
  .option('--name <name>', 'Project name')
  .option('--brokers <brokers>', 'Kafka brokers')
  .option('--docker', 'Include docker-compose.yaml')
  .action(initCommand);

// Topics commands
const topics = program.command('topics').description('Manage Kafka topics');

topics
  .command('create')
  .description('Create topics from configuration')
  .option('--config <path>', 'Path to topics config', 'src/topics.ts')
  .option('--dry-run', 'Show what would be created')
  .action(createTopicsCommand);

topics
  .command('list')
  .description('List all topics')
  .option('--filter <pattern>', 'Filter topics by pattern')
  .option('--json', 'Output as JSON')
  .action(listTopicsCommand);

// Consumer commands
const consumer = program.command('consumer').description('Consumer management');

consumer
  .command('lag <group-id>')
  .description('Check consumer group lag')
  .option('--topic <topic>', 'Filter by topic')
  .option('--watch', 'Continuously watch lag')
  .action(consumerLagCommand);

// Health command
program
  .command('health')
  .description('Check Kafka cluster health')
  .option('--json', 'Output as JSON')
  .action(healthCommand);

// Generate commands
const generate = program.command('generate').description('Code generation');

generate
  .command('types')
  .description('Generate TypeScript types from schemas')
  .option('--config <path>', 'Path to topics config', 'src/topics.ts')
  .option('--output <path>', 'Output file path', 'src/topics.generated.ts')
  .action(generateTypesCommand);

export { program };
```

```typescript
// src/index.ts
#!/usr/bin/env node
import { program } from './cli';

program.parse();
```

## Testing Strategy

```typescript
// __tests__/commands/init.test.ts
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs/promises';
import { initCommand } from '../../src/commands/init';

describe('init command', () => {
  beforeEach(async () => {
    // Setup test directory
  });
  
  afterEach(async () => {
    // Cleanup
  });
  
  it('should create kafka.config.ts', async () => {
    await initCommand({ name: 'test-app', brokers: 'localhost:9092' });
    
    const exists = await fs.access('src/kafka.config.ts').then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });
  
  it('should create topics.ts', async () => {
    await initCommand({ name: 'test-app', brokers: 'localhost:9092' });
    
    const exists = await fs.access('src/topics.ts').then(() => true).catch(() => false);
    expect(exists).toBe(true);
  });
});
```

## Installation & Usage

```bash
# Install globally
pnpm add -g @gatwech/kafka-cli

# Or use with npx
npx @gatwech/kafka-cli init

# Or install as dev dependency
pnpm add -D @gatwech/kafka-cli
```

## Next Steps

1. Create package structure
2. Implement core commands
3. Add tests
4. Write documentation
5. Publish to npm
