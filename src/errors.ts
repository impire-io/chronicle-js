// The errors the SDK raises. A service's refusal is a ServiceError carrying
// a code from the contract's catalog; everything else is a typed client
// error a caller can tell apart with instanceof — the same distinctions the
// Go client draws with its sentinel errors (design 12 § the conformance
// suite matches on them).

/** The base of every error the SDK raises. */
export class ChronicleError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** A service answered with a catalogued error (Nats-Service-Error-Code). */
export class ServiceError extends ChronicleError {
  constructor(
    /** The code from the contract's error catalog. */
    readonly code: string,
    /** The service's description of what went wrong. */
    readonly description: string,
  ) {
    super(`${code}: ${description}`);
  }
}

/** Nobody answered: the node or the index is not running for this account. */
export class NoResponderError extends ChronicleError {
  constructor(readonly subject: string) {
    super(`${subject}: no responder (is the node running for this account?)`);
  }
}

/** A birth found the thing already there. */
export class ThingExistsError extends ChronicleError {}

/** A guarded append found the thing moved past the expected sequence: re-read, re-validate, retry. */
export class ThingMovedError extends ChronicleError {}

/** A saved version found the log moved past the sequence it covers. */
export class StaleVersionError extends ChronicleError {}

/** A write refused before it was sent: the type's rules say no (preflight). */
export class PreflightError extends ChronicleError {}

/** The payload or state fails the declared JSON Schema. */
export class SchemaViolationError extends PreflightError {}

/** The thing sits under an aspect its parent type does not declare. */
export class UndeclaredAspectError extends PreflightError {}

/** The thing's type defines no such operation. */
export class UndefinedOperationError extends PreflightError {}

/** A streamed reply went quiet past the stall timeout. */
export class StreamStalledError extends ChronicleError {}

/** A streamed reply skipped a chunk, or its trailer disagrees with what arrived. */
export class StreamGapError extends ChronicleError {}

/** A read named something that is not there: a thing with no state, an undefined type, an undeclared index. */
export class NotFoundError extends ChronicleError {}

/** A name fails its grammar (design 02 § subject grammar). */
export class InvalidNameError extends ChronicleError {}
