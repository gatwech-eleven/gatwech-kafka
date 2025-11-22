import path from "path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    clearMocks: true,
    testTimeout: 10000,
    include: [
      "__tests__/**/*.test.ts",
      "__tests__/**/*.spec.ts",
      "test/**/*.test.ts",
      "test/**/*.spec.ts",
      "__test__/**/*.test.ts",
      "src/**/__tests__/**/*.test.ts",
      "src/**/__tests__/**/*.spec.ts",
    ],
    setupFiles: [path.resolve(__dirname, "__tests__/setup.ts")],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: ["node_modules/", "dist/", "**/__tests__/**", "src/server.ts"],
    },
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
});
