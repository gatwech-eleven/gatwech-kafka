import type { Logger } from "winston";
import { createLogger, format, transports } from "winston";

const { combine, timestamp, printf, colorize } = format;

const defaultFormat = printf(
	({ level, message, timestamp, service, ...meta }) => {
		const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
		return `${timestamp} [${service || "kafka"}] ${level}: ${message}${metaStr}`;
	},
);

export function createServiceLogger(serviceName = "kafka"): Logger {
	return createLogger({
		level:
			process.env.LOG_LEVEL ||
			(process.env.NODE_ENV === "production" ? "info" : "debug"),
		format: combine(
			timestamp(),
			colorize({ all: true }),
			format((info) => ({ ...info, service: serviceName }))(),
			defaultFormat,
		),
		transports: [new transports.Console()],
	});
}

const loggerInstance = createServiceLogger(process.env.SERVICE_NAME || "kafka");

export const logger: Logger = loggerInstance;
