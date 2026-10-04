import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/", ".chronicle/", "node_modules/", "src/generated/"] },
  js.configs.recommended,
  {
    files: ["**/*.ts"],
    extends: [tseslint.configs.strictTypeChecked],
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      "@typescript-eslint/restrict-template-expressions": ["error", { allowNumber: true }],
    },
  },
  {
    files: ["**/*.mjs", "eslint.config.js"],
    languageOptions: { globals: globals.node },
  },
);
