// JSON Schema, as the type records declare it. The Go contract compiles
// with santhosh-tekuri/jsonschema, draft 2020-12 by default; Ajv's 2020
// build reads the same documents.
import { Ajv2020, type ValidateFunction } from "ajv/dist/2020.js";

const ajv = new Ajv2020({ strict: false, allErrors: false, validateFormats: false });
const compiled = new Map<string, ValidateFunction>();

/** A compiled schema: validate(value) returns an error message, or "" when the value passes. */
export type Validator = (value: unknown) => string;

/** Compiles a JSON Schema document, cached by its text; throws when it does not compile. */
export function compileSchema(schema: unknown): Validator {
  if (schema === undefined) {
    throw new Error("no schema");
  }
  const key = JSON.stringify(schema);
  let fn = compiled.get(key);
  if (fn === undefined) {
    fn = ajv.compile(schema as object | boolean);
    compiled.set(key, fn);
  }
  const validate = fn;
  return (value) => (validate(value) ? "" : ajv.errorsText(validate.errors));
}
