import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import chalk from 'chalk';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function generateConsumer(name: string, options: { topic?: string, group?: string }) {
  const topic = options.topic || `${name.toLowerCase()}-events`;
  const groupId = options.group || `${name.toLowerCase()}-group`;
  
  const templatePath = path.join(__dirname, '../../../templates/consumer.ts.template');
  const template = await fs.readFile(templatePath, 'utf-8');
  
  const content = template
    .replace(/\{\{NAME\}\}/g, name)
    .replace(/\{\{TOPIC\}\}/g, topic)
    .replace(/\{\{GROUP_ID\}\}/g, groupId);
  
  const targetDir = path.resolve(process.cwd(), 'src/consumers');
  await fs.mkdir(targetDir, { recursive: true });
  
  const targetFile = path.join(targetDir, `${name.toLowerCase()}.ts`);
  await fs.writeFile(targetFile, content);
  
  console.log(chalk.green(`✔ Consumer generated: ${chalk.bold(targetFile)}`));
}
