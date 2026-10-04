// RFC 7386 JSON Merge Patch — the effect merge (decision 0011), as the Go
// contract applies it: a patch that is not an object replaces the target;
// a target that is not an object is treated as empty; null deletes.

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

/** Applies patch to target and returns the result; neither input is changed. */
export function mergePatch(target: unknown, patch: unknown): unknown {
  if (!isObject(patch)) {
    return structuredClone(patch);
  }
  return mergeObjects(isObject(target) ? structuredClone(target) : {}, patch);
}

function mergeObjects(target: Record<string, unknown>, patch: Record<string, unknown>): Record<string, unknown> {
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) {
      // eslint-disable-next-line @typescript-eslint/no-dynamic-delete -- RFC 7386: null deletes the member
      delete target[key];
      continue;
    }
    if (isObject(value)) {
      const current = target[key];
      target[key] = mergeObjects(isObject(current) ? current : {}, value);
      continue;
    }
    target[key] = structuredClone(value);
  }
  return target;
}
