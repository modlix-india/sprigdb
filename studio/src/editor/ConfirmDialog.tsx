import { useEffect, useId, useRef } from 'react';
import { FormattedMessage } from 'react-intl';

export interface ConfirmRequest {
  /** The statement(s), as they will run. */
  sql: string;
  /** read, write, ddl, ... (lib/sql's classify). */
  kind: string;
  /** Where it runs: `tenant / database`. */
  target: string;
  /** A line above the statement (who proposed it). */
  note?: string;
}

/** The "Run this statement?" dialog of query tabs, for statements that
 * come from elsewhere (the assistant's proposals). Cancel has the focus. */
export function ConfirmDialog({ request, onResult }: { request: ConfirmRequest | null; onResult: (ok: boolean) => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  const cancel = useRef<HTMLButtonElement>(null);
  const title = useId();
  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (request && !d.open) {
      d.showModal?.();
      // showModal focuses the first button; the safe choice has it.
      cancel.current?.focus();
    }
    if (!request && d.open) d.close();
  }, [request]);
  return (
    <dialog ref={ref} aria-labelledby={title} onCancel={() => onResult(false)} data-testid="confirm-dialog">
      {request && (
        <div className="stack">
          <h2 id={title}>
            <FormattedMessage id="query.confirmTitle" />
          </h2>
          {request.note && <p className="muted">{request.note}</p>}
          <p>
            <FormattedMessage id="query.confirmBody" values={{ kind: request.kind, target: request.target }} />
          </p>
          <pre>{request.sql}</pre>
          <div className="row">
            <button className="primary" onClick={() => onResult(true)}>
              <FormattedMessage id="query.confirm" />
            </button>
            <button ref={cancel} onClick={() => onResult(false)}>
              <FormattedMessage id="query.cancelConfirm" />
            </button>
          </div>
        </div>
      )}
    </dialog>
  );
}
