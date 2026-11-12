import type { IHeaders, Producer } from "kafkajs";

const messageOf = (error: unknown) =>
	error instanceof Error ? error.message : String(error);

const textOf = (value: Buffer | string | undefined) =>
	typeof value === "string" ? value : (value?.toString() ?? "");

/**
 * Why a message goes to the DLQ: the handler failed on every attempt, or the
 * topic's schema rejects the message
 */
export type DlqFailure =
	| { reason: "handler"; error: unknown }
	| {
			reason: "schema";
			issues: { path: string; message: string; code: string }[];
	  };

/**
 * The fields of a DLQ entry that say why the message failed
 */
function describeFailure(failure: DlqFailure) {
	if (failure.reason === "schema") {
		const summary = failure.issues
			.map((issue) =>
				issue.path ? `${issue.path}: ${issue.message}` : issue.message,
			)
			.join("; ");
		return {
			reason: failure.reason,
			errorMessage: `The message doesn't match the topic's schema: ${summary}`,
			validationIssues: failure.issues,
		};
	}
	const { error } = failure;
	return {
		reason: failure.reason,
		errorMessage: messageOf(error),
		stackTrace: error instanceof Error ? error.stack : undefined,
		errorCause:
			error instanceof Error && error.cause !== undefined
				? messageOf(error.cause)
				: undefined,
	};
}

/**
 * Publishes a message that couldn't be handled to the DLQ
 * @description The entry holds the message as it was, where it came from and
 * why it failed: the handler's last error with its stack and cause, or the
 * issues the topic's schema raised. It is keyed by the message's key, or by its
 * offset when it has none. What the producer throws is thrown
 */
export async function publishToDlq(options: {
	producer: Producer;
	dlqTopic: string;
	message: {
		value: Buffer | null;
		key: Buffer | null;
		headers?: IHeaders | undefined;
	};
	failure: DlqFailure;
	/**
	 * Where the message came from
	 */
	origin: { topic: string; partition: number; offset: string; groupId: string };
}) {
	const { producer, dlqTopic, message, failure, origin } = options;

	const text = message.value?.toString();
	let originalValue: unknown = text;
	if (text !== undefined) {
		try {
			originalValue = JSON.parse(text);
		} catch {
			// Not JSON: the entry keeps the text
		}
	}
	const originalKey = message.key?.toString();

	const entry = {
		timestamp: Date.now(),
		originalTopic: origin.topic,
		originalPartition: origin.partition,
		originalOffset: origin.offset,
		consumerGroup: origin.groupId,
		...describeFailure(failure),
		originalKey,
		originalHeaders: Object.fromEntries(
			Object.entries(message.headers ?? {}).map(([name, value]) => [
				name,
				textOf(Array.isArray(value) ? value[value.length - 1] : value),
			]),
		),
		originalValue,
	};

	await producer.send({
		topic: dlqTopic,
		messages: [
			{
				key: originalKey || origin.offset,
				value: JSON.stringify(entry),
				headers: {
					"x-original-topic": origin.topic,
					"x-original-offset": origin.offset,
					"x-original-partition": String(origin.partition),
				},
			},
		],
	});
}
