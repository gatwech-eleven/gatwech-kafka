import { vi } from "vitest";

declare global {
  var __appMocks: any;
}

// Use fake timers to speed up retry/backoff behavior in tests.
vi.useFakeTimers();
