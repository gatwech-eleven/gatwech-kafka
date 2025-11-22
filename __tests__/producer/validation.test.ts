import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { validateMessage } from "../../src/validation";
import { ResetPasswordRequestEmailSchema } from "../../src/schemas/user-schemas";
import { logger } from "../../src/utils/logger";

describe("validation.validateMessage", () => {
  let warnSpy: any;

  beforeEach(() => {
    warnSpy = vi
      .spyOn(logger as any, "warn")
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      .mockImplementation((_msg: any, _meta?: any) => logger as any);
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it("returns success for a valid email reset payload", () => {
    const payload = {
      identifier: "email",
      email: "test@example.com",
      token: "tok_123",
      timestamp: Date.now(),
    };

    const res = validateMessage(ResetPasswordRequestEmailSchema, payload);
    expect(res.success).toBe(true);
    if (res.success) {
      expect(res.data.email).toBe(payload.email);
      expect(res.data.token).toBe(payload.token);
      expect(res.data.timestamp).toBeDefined(); // Ensure timestamp is present
    }
  });

  it("returns error for invalid payload (missing email)", () => {
    const payload = {
      identifier: "email",
      token: "tok_123",
      timestamp: Date.now(),
    };

    const res = validateMessage(ResetPasswordRequestEmailSchema, payload);
    expect(res.success).toBe(false);
    if (!res.success) {
      expect(res.error).toBeDefined();
      // error should include information about the missing 'email' field
      const err: any = res.error as any;
      expect(err.properties).toBeDefined();
      expect(err.properties.email).toBeDefined();
      expect(Array.isArray(err.properties.email.errors)).toBe(true);
    }
    expect(warnSpy).toHaveBeenCalled();
  });
});
