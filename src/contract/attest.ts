// The attestation (design 02 § the operation record, decision 0057): a node
// that wrote an operation for a caller signs the record's signed form with
// its nkey and adds Op-Attester and Op-Signature; a reader rebuilds the form
// from the subject, the headers and the payload and verifies it against
// the attester's public key — which must be one the store's META lists, a
// question answered before this one.
import { fromPublic } from "@nats-io/nkeys";
import { HDR, type Op } from "./record.js";

const encoder = new TextEncoder();

/** The fields the signed form is built from. */
export type Attestable = Pick<Op, "subject" | "id" | "type" | "author" | "parents" | "ts" | "version" | "payload">;

/**
 * The signed form: the subject, the message id, the five record headers
 * and the hex SHA-256 of the payload, one per line — Op-Ts as sent (empty
 * when unset), Op-Version defaulted. Byte for byte the Go contract's
 * `Op.SignedForm`, which the conformance fixture pins.
 */
export async function signedForm(op: Attestable): Promise<string> {
  // A copy: the payload may sit in a shared buffer, which the digest API refuses.
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(op.payload)));
  const hex = Array.from(digest, (b) => b.toString(16).padStart(2, "0")).join("");
  return [
    op.subject,
    op.id,
    op.type,
    op.author,
    op.parents.join(","),
    op.ts,
    op.version === "" ? "1" : op.version,
    hex,
  ].join("\n");
}

/**
 * Whether the signature verifies against the attester public key the
 * operation names. The signature only: whether that key is trusted — an
 * attester key in META — is the reader's question.
 */
export async function verifyAttestation(op: Attestable & Pick<Op, "attester" | "signature">): Promise<boolean> {
  if (op.attester === "" || op.signature === "") {
    return false;
  }
  let signature: Uint8Array;
  let key: ReturnType<typeof fromPublic>;
  try {
    signature = Uint8Array.from(atob(op.signature), (c) => c.charCodeAt(0));
    key = fromPublic(op.attester);
  } catch {
    return false;
  }
  try {
    return key.verify(encoder.encode(await signedForm(op)), signature);
  } catch {
    return false;
  }
}

/** The attestation headers an operation carries, when it does. */
export function attestationHeaders(op: Pick<Op, "attester" | "signature">): [string, string][] {
  return op.attester !== "" && op.signature !== ""
    ? [
        [HDR.attester, op.attester],
        [HDR.signature, op.signature],
      ]
    : [];
}
