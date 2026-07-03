vi.mock("nanoid", () => ({ nanoid: (len = 12) => "fixedid00000" }));

import { vi } from "vitest";
import { describe } from "node:test";
import { it } from "vitest";
import { expect } from "vitest";
import { generateCorrelationId } from "../../src/utils/helpers";

describe("generateCorrelationId", () => {
  it("returns random id when no prefix provided", () => {
    const id = generateCorrelationId();
    expect(typeof id).toBe("string");
    expect(id).toBe("fixedid00000");
  });

  it("prepends prefix when provided", () => {
    const id = generateCorrelationId("reg");
    expect(id).toBe("reg_fixedid00000");
  });
});
