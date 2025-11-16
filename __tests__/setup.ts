import { vi } from "vitest";

declare global {
  var __appMocks: any;
}

vi.useFakeTimers();
