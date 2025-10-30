import { describe, it, expect } from "vitest";
import { validateMessage } from "../src/validation";
import { ResetPasswordRequestEmailSchema } from "../src/schemas/user-schemas";

describe("validation.validateMessage", () => {
  it("returns success for a valid email reset payload", () => {
    const payload = {
      identifier: "email",
      email: "test@example.com",
      token: "tok_123",
    };

    const res = validateMessage(ResetPasswordRequestEmailSchema, payload);
    expect(res.success).toBe(true);
    if (res.success) {
      expect(res.data.email).toBe(payload.email);
      expect(res.data.token).toBe(payload.token);
    }
  });

  it("returns error for invalid payload (missing email)", () => {
    const payload = {
      identifier: "email",
      token: "tok_123",
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
  });
});
