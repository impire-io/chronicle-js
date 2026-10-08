// Name grammars and the derivations every subject, stream, bucket and META
// key is built from (design 02, design 03) — the same rules as the Go
// contract package, read from the contract artifact. The words are the
// user's (chronicle-hq decision 0044): a store, an instance named by a
// path, children; the protocol tokens underneath (LOG_, STATE_, the log.
// META prefix) do not change with the words.
import { InvalidNameError } from "../errors.js";
import { GRAMMARS } from "../generated/contract.js";

const namePattern = new RegExp(GRAMMARS.name.pattern);
const instanceToken = new RegExp(GRAMMARS.instanceToken.pattern);
const reserved = new Set<string>(GRAMMARS.reservedStoreNames);
const root = GRAMMARS.root;

/** How a user writes a path, and how it is stored. */
export const PATH_SEPARATOR = GRAMMARS.path.separator;
export const TAIL_SEPARATOR = GRAMMARS.path.storedSeparator;

/** The kinds of name the grammar validates. */
export type NameKind = "store" | "index" | "type" | "principal" | "instance" | "path";

/** Throws InvalidNameError unless store is a valid, unreserved store name. */
export function validateStoreName(store: string): void {
  if (!namePattern.test(store)) {
    throw new InvalidNameError(`store name ${JSON.stringify(store)}: must match [a-z0-9-]+`);
  }
  if (reserved.has(store)) {
    throw new InvalidNameError(`store name ${JSON.stringify(store)} is reserved`);
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

/** Throws InvalidNameError unless tail is a stored instance tail: one or more token-safe segments joined with '.'. */
export function validateInstance(tail: string): void {
  if (tail === "") {
    throw new InvalidNameError("instance: must be one or more tokens");
  }
  for (const tok of tail.split(TAIL_SEPARATOR)) {
    if (tok === "") {
      throw new InvalidNameError(`instance ${JSON.stringify(tail)}: empty token`);
    }
    if (!instanceToken.test(tok)) {
      throw new InvalidNameError(`instance token ${JSON.stringify(tok)}: must match [a-zA-Z0-9_-]+`);
    }
  }
}

/**
 * Throws InvalidNameError unless path is the user's spelling of an
 * instance: type/id, then name/id for each child. A dotted spelling is
 * refused with the grammar named — one spelling, never two.
 */
export function validatePath(path: string): void {
  if (path === "") {
    throw new InvalidNameError("path: must be type/id, with /name/id for each child");
  }
  for (const seg of path.split(PATH_SEPARATOR)) {
    if (seg === "") {
      throw new InvalidNameError(
        `path ${JSON.stringify(path)}: empty segment (a path is type/id, with /name/id for each child)`,
      );
    }
    if (!instanceToken.test(seg)) {
      if (seg.includes(TAIL_SEPARATOR)) {
        throw new InvalidNameError(
          `path ${JSON.stringify(path)}: segments are separated by "/", not "." (a path is type/id, with /name/id for each child)`,
        );
      }
      throw new InvalidNameError(`path segment ${JSON.stringify(seg)}: must match [a-zA-Z0-9_-]+`);
    }
  }
}

/** The tail a path is stored under: the same segments, dotted. Validates the path first. */
export function pathTail(path: string): string {
  validatePath(path);
  return path.replaceAll(PATH_SEPARATOR, TAIL_SEPARATOR);
}

/** The path a stored tail reads as. */
export const tailPath = (tail: string): string => tail.replaceAll(TAIL_SEPARATOR, PATH_SEPARATOR);

/** The type a path names: its first segment. */
export const pathType = (path: string): string => path.split(PATH_SEPARATOR)[0] ?? path;

const validators: Record<NameKind, (name: string) => void> = {
  store: validateStoreName,
  index: validateIndexName,
  type: validateTypeName,
  principal: validatePrincipalName,
  instance: validateInstance,
  path: validatePath,
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

/** The store name uppercased with '-' mapped to '_'. */
export const upperStore = (store: string): string => store.replaceAll("-", "_").toUpperCase();
/** The store's stream, LOG_<STORE>. */
export const streamName = (store: string): string => `LOG_${upperStore(store)}`;
/** The store's state bucket, STATE_<STORE>. */
export const stateBucket = (store: string): string => `STATE_${upperStore(store)}`;
/** Every subject of the store. */
export const storeSubjects = (store: string): string => `${root}.${store}.>`;
/** Every op subject of the store. */
export const opsFilter = (store: string): string => `${root}.${store}.OPS.>`;
/** The subject an instance's history lives on, from its stored tail. */
export const opsSubject = (store: string, tail: string): string => `${root}.${store}.OPS.${tail}`;
/** The prefix of every op subject of the store. */
export const opsPrefix = (store: string): string => `${root}.${store}.OPS.`;
/** The instance tail an op subject names. */
export const instanceFromSubject = (store: string, subject: string): string =>
  subject.startsWith(opsPrefix(store)) ? subject.slice(opsPrefix(store).length) : subject;

/** META key of a store's config. */
export const metaStoreConfig = (store: string): string => `log.${store}.config`;
/** META key of a type record. */
export const metaStoreType = (store: string, type: string): string => `log.${store}.type.${type}`;
/** META key of an index declaration. */
export const metaIndex = (store: string, index: string): string => `index.${store}.${index}`;
/** META key of a principal. */
export const metaPrincipal = (id: string): string => `identity.principal.${id}`;
/** META key of a membership. */
export const metaMember = (id: string): string => `identity.member.${id}`;
/** META key of an invite. */
export const metaInvite = (digest: string): string => `identity.invite.${digest}`;
