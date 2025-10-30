import { describe, it, expect, vi } from 'vitest';
import { validateAndSend, validateAndSendMany } from '../src/producer/validate-and-send';
import {
  ResetPasswordRequestEmailSchema,
} from '../src/schemas/user-schemas';

describe('producer.validateAndSend helpers', () => {
  it('validateAndSend sends a valid message', async () => {
    const fakeClient = {
      sendMessage: vi.fn().mockResolvedValue(undefined),
    } as unknown as any;

    const message = {
      key: 'k1',
      value: { identifier: 'email', email: 'ok@example.com', token: 'tok' },
      headers: {},
    };

    await validateAndSend(fakeClient, 'topic.test', ResetPasswordRequestEmailSchema, message);

    expect(fakeClient.sendMessage).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessage).toHaveBeenCalledWith('topic.test', message);
  });

  it('validateAndSend throws on invalid message', async () => {
    const fakeClient = { sendMessage: vi.fn() } as unknown as any;
    const message = { key: 'k2', value: { identifier: 'email', token: 'no-email' } } as any;

    await expect(
      validateAndSend(fakeClient, 'topic.test', ResetPasswordRequestEmailSchema, message),
    ).rejects.toBeDefined();
  });

  it('validateAndSendMany sends a valid batch', async () => {
    const fakeClient = {
      sendMessages: vi.fn().mockResolvedValue(undefined),
    } as unknown as any;

    const messages = [
      { key: 'k1', value: { identifier: 'email', email: 'a@b.com', token: 't1' } },
      { key: 'k2', value: { identifier: 'email', email: 'c@d.com', token: 't2' } },
    ];

    await validateAndSendMany(fakeClient, 'topic.batch', ResetPasswordRequestEmailSchema, messages as any);

    expect(fakeClient.sendMessages).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessages).toHaveBeenCalledWith('topic.batch', messages);
  });

  it('validateAndSendMany throws when any message invalid', async () => {
    const fakeClient = { sendMessages: vi.fn() } as unknown as any;
    const messages = [
      { key: 'k1', value: { identifier: 'email', email: 'a@b.com', token: 't1' } },
      { key: 'k2', value: { identifier: 'email', token: 'missing' } },
    ];

    await expect(
      validateAndSendMany(fakeClient, 'topic.batch', ResetPasswordRequestEmailSchema, messages as any),
    ).rejects.toBeDefined();
  });
});
