import path from "path";
import { pathToFileURL } from "url";
import chalk from "chalk";
import "dotenv/config";

export async function createTopics() {
  const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");
  const topicsPath = path.resolve(process.cwd(), "src/topics.ts");

  try {
    const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);
    const { topics } = await import(pathToFileURL(topicsPath).href);

    if (!kafka || !topics) {
      console.error(chalk.red("Error: Could not find kafka instance or topics definition."));
      return;
    }

    console.log(chalk.blue("Connecting to Kafka..."));
    await kafka.connect();
    
    console.log(chalk.blue("Provisioning topics..."));
    await kafka.provisionTopics();
    
    console.log(chalk.green("✔ Topics provisioned successfully."));
    await kafka.disconnect();
  } catch (error: any) {
    console.error(chalk.red(`Error: ${error.message}`));
    if (error.code === "ERR_MODULE_NOT_FOUND") {
      console.log(chalk.yellow("Hint: Ensure 'src/kafka.config.ts' and 'src/topics.ts' exist. Run 'kafka-cli init' to create them."));
    }
  }
}
