import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import chalk from "chalk";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function generateProducer(
	name: string,
	options: { topic?: string },
) {
	const topic = options.topic || `${name.toLowerCase()}-events`;

	const templatePath = path.join(
		__dirname,
		"../../../templates/producer.ts.template",
	);
	const template = await fs.readFile(templatePath, "utf-8");

	const content = template
		.replace(/\{\{NAME\}\}/g, name)
		.replace(/\{\{TOPIC\}\}/g, topic);

	const targetDir = path.resolve(process.cwd(), "src/producers");
	await fs.mkdir(targetDir, { recursive: true });

	const targetFile = path.join(targetDir, `${name.toLowerCase()}.ts`);
	await fs.writeFile(targetFile, content);

	console.log(chalk.green(`✔ Producer generated: ${chalk.bold(targetFile)}`));
}
