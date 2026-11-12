export function resolveBrokersFromEnv(): string[] {
	const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
	if (raw && raw.trim().length > 0) {
		return raw
			.split(",")
			.map((s) => s.trim())
			.filter(Boolean);
	}

	if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") {
		return ["kafka:9092"];
	}

	return ["localhost:9092"];
}

export default resolveBrokersFromEnv;
