import { Sparkles } from '../icons';
import { ask, useRoles, type AskOptions } from './store';
import type { AiRole } from './thread';

/** An entry point to the assistant beside what it asks about. Hidden when
 * the role isn't available to this user (no provider, or the assistants
 * are off). */
export function AskButton({
  label,
  role,
  options,
  testId,
  className,
}: {
  label: string;
  role: AiRole;
  options: () => Omit<AskOptions, 'role'>;
  testId?: string;
  className?: string;
}) {
  const roles = useRoles();
  if (!roles.includes(role)) return null;
  return (
    <button className={`ai-ask${className ? ` ${className}` : ''}`} data-testid={testId} onClick={() => ask({ ...options(), role })}>
      <Sparkles size={13} aria-hidden="true" />
      {label}
    </button>
  );
}
