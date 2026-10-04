import { defineConfig } from "vitest/config";

// The managed half: needs a managed chronicle binary (CHRONICLE_MANAGED_BIN).
export default defineConfig({
  test: {
    include: ["test/managed/**/*.managed.ts"],
    testTimeout: 180_000,
    hookTimeout: 120_000,
  },
});
