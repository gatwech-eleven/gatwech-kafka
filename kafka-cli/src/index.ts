#!/usr/bin/env node
import { Command } from 'commander';
import { initCommand } from './commands/init.js';
import { listTopics } from './commands/topics/list.js';
import { createTopics } from './commands/topics/create.js';
import { describeTopic } from './commands/topics/describe.js';
import { startConsumer } from './commands/consumer/start.js';
import { generateConsumer } from './commands/generate/consumer.js';
import { generateProducer } from './commands/generate/producer.js';

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

// Topics command
const topics = program.command('topics').description('Manage Kafka topics');

topics
  .command('list')
  .description('List configured topics')
  .action(listTopics);

topics
  .command('create')
  .description('Create missing topics')
  .action(createTopics);

topics
  .command('describe <topic>')
  .description('Describe a topic metadata')
  .action(describeTopic);

// Consumer command
const consumer = program.command('consumer').description('Manage Kafka consumers');

consumer
  .command('start <topic>')
  .description('Start a simple consumer for a topic')
  .option('--group <group>', 'Consumer group ID')
  .action(startConsumer);

// Generate command
const generate = program.command('generate').description('Generate boilerplate code');

generate
  .command('consumer <name>')
  .description('Generate a new consumer')
  .option('--topic <topic>', 'Kafka topic')
  .option('--group <group>', 'Consumer group ID')
  .action(generateConsumer);

generate
  .command('producer <name>')
  .description('Generate a new producer')
  .option('--topic <topic>', 'Kafka topic')
  .action(generateProducer);

program.parse();
