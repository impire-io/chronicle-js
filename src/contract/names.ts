// Name grammars and the derivations every subject, stream, bucket and META
// key is built from (design 02, design 03) — the same rules as the Go
// contract package, read from the contract artifact.
import { InvalidNameError } from "../errors.js";
import { GRAMMARS } from "../generated/contract.js";

const namePattern = new RegExp(GRAMMARS.name.pattern);
const thingToken = new RegExp(GRAMMARS.thingToken.pattern);
const reserved = new Set<string>(GRAMMARS.reservedLogNames);
const root = GRAMMARS.root;

/** The kinds of name the grammar validates. */
export type NameKind = "log" | "index" | "type" | "principal" | "thing";

/** Throws InvalidNameError unless log is a valid, unreserved log name. */
export function validateLogName(log: string): void {
  if (!namePattern.test(log)) {
    throw new InvalidNameError(`log name ${JSON.stringify(log)}: must match [a-z0-9-]+`);
  }
  if (reserved.has(log)) {
    throw new InvalidNameError(`log name ${JSON.stringify(log)} is reserved`);
  }
}

/** Throws InvalidNameError unless index is a valid index name. */
export function validateIndexName(index: string): void {
  if (!namePattern.test(index)) {
    throw new InvalidNameError(`index name ${JSON.stringify(index)}: must match [a-z0-9-]+`);
  }
}

/** Throws InvalidNameError unless name is a valid type name. */
export function validateTypeName(name: string): void {
  if (!namePattern.test(name)) {
    throw new InvalidNameError(`type name ${JSON.stringify(name)}: must match [a-z0-9-]+`);
  }
}

/** Throws InvalidNameError unless name is a valid principal, and not the account's own service. */
export function validatePrincipalName(name: string): void {
  if (!namePattern.test(name)) {
    throw new InvalidNameError(`principal ${JSON.stringify(name)}: must match [a-z0-9-]+`);
  }
  if (name === GRAMMARS.servicePrincipal) {
    throw new InvalidNameError(`principal ${JSON.stringify(name)} is reserved for the account's own service`);
  }
}

/** Throws InvalidNameError unless thing is one or more subject-token-safe tokens joined with '.'. */
export function validateThing(thing: string): void {
  if (thing === "") {
    throw new InvalidNameError("thing: must be one or more tokens");
  }
  for (const tok of thing.split(".")) {
    if (tok === "") {
      throw new InvalidNameError(`thing ${JSON.stringify(thing)}: empty token`);
    }
    if (!thingToken.test(tok)) {
      throw new InvalidNameError(`thing token ${JSON.stringify(tok)}: must match [a-zA-Z0-9_-]+`);
    }
  }
}

const validators: Record<NameKind, (name: string) => void> = {
  log: validateLogName,
  index: validateIndexName,
  type: validateTypeName,
  principal: validatePrincipalName,
  thing: validateThing,
};

/** Whether name passes the grammar of its kind. */
export function isValidName(kind: NameKind, name: string): boolean {
  try {
    validators[kind](name);
    return true;
  } catch {
    return false;
  }
}

/** The log name uppercased with '-' mapped to '_'. */
export const upperLog = (log: string): string => log.replaceAll("-", "_").toUpperCase();
/** The log's stream, LOG_<LOG>. */
export const streamName = (log: string): string => `LOG_${upperLog(log)}`;
/** The log's state bucket, STATE_<LOG>. */
export const stateBucket = (log: string): string => `STATE_${upperLog(log)}`;
/** Every subject of the log. */
export const logSubjects = (log: string): string => `${root}.${log}.>`;
/** Every op subject of the log. */
export const opsFilter = (log: string): string => `${root}.${log}.OPS.>`;
/** The subject a thing's ops live on. */
export const opsSubject = (log: string, thing: string): string => `${root}.${log}.OPS.${thing}`;
/** The prefix of every op subject of the log. */
export const opsPrefix = (log: string): string => `${root}.${log}.OPS.`;
/** The thing an op subject names. */
export const thingFromSubject = (log: string, subject: string): string =>
  subject.startsWith(opsPrefix(log)) ? subject.slice(opsPrefix(log).length) : subject;

/** META key of a log's config. */
export const metaLogConfig = (log: string): string => `log.${log}.config`;
/** META key of a type record. */
export const metaLogType = (log: string, type: string): string => `log.${log}.type.${type}`;
/** META key of an index declaration. */
export const metaIndex = (log: string, index: string): string => `index.${log}.${index}`;
/** META key of a principal. */
export const metaPrincipal = (id: string): string => `identity.principal.${id}`;
/** META key of a membership. */
export const metaMember = (id: string): string => `identity.member.${id}`;
/** META key of an invite. */
export const metaInvite = (digest: string): string => `identity.invite.${digest}`;
