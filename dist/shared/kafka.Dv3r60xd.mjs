import { format, createLogger, transports } from 'winston';

const { combine, timestamp, printf, colorize } = format;
const defaultFormat = printf(
  ({ level, message, timestamp: timestamp2, service, ...meta }) => {
    const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
    return `${timestamp2} [${service || "kafka"}] ${level}: ${message}${metaStr}`;
  }
);
function createServiceLogger(serviceName = "kafka") {
  return createLogger({
    level: process.env.LOG_LEVEL || (process.env.NODE_ENV === "production" ? "info" : "debug"),
    format: combine(
      timestamp(),
      colorize({ all: true }),
      format((info) => ({ ...info, service: serviceName }))(),
      defaultFormat
    ),
    transports: [new transports.Console()]
  });
}
const loggerInstance = createServiceLogger(process.env.SERVICE_NAME || "kafka");
const logger = loggerInstance;

export { logger as l };
