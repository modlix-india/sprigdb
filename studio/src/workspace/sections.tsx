// The activity bar's sections: today's pages regrouped into a
// few sections, each with the pages it holds. Routes are unchanged.
// Which sections a user sees, and where, is up to their workspace
// (workspaces.ts).

import type { ComponentType, SVGProps } from 'react';
import type { MessageId } from '../i18n/en';
import {
  BookOpen,
  DataCentreIcon,
  LayoutDashboard,
  Database,
  GitFork,
  Send,
  ServerCog,
  History,
  Settings,
  Shield,
  ShieldCheck,
  Star,
  SquareFunction,
  SquareTerminal,
  Zap,
} from '../icons';

type Icon = ComponentType<SVGProps<SVGSVGElement> & { size?: number }>;

export type SectionId =
  | 'explorer'
  | 'query'
  | 'saved'
  | 'history'
  | 'dashboards'
  | 'messaging'
  | 'logic'
  | 'performance'
  | 'inheritance'
  | 'access'
  | 'operations'
  | 'security'
  | 'docs'
  | 'operator'
  | 'settings';

export interface SectionLink {
  label: MessageId;
  /** Path relative to the tenant base (`/t/<tenant>/`), or absolute when it starts with `/`. */
  to: string;
  end?: boolean;
  /** Shows the approvals count. */
  badge?: boolean;
}

export interface Section {
  id: SectionId;
  label: MessageId;
  icon: Icon;
  /** Where the activity icon goes (relative to the tenant base, or absolute);
   * null when the section has no page of its own and opens its first link. */
  to: string | null;
  /** Pages this section holds, listed in its side panel. */
  links: SectionLink[];
  /** Whether a path relative to the tenant base belongs to this section. */
  owns: (rel: string) => boolean;
  /** At the bottom of the activity bar. */
  bottom?: boolean;
  operatorOnly?: boolean;
  /** A section that only switches the side panel (no page of its own). */
  panel?: 'saved' | 'history';
  /** A page section whose side panel is its own list instead of links and the database tree. */
  side?: 'dashboards';
  testId?: string;
}

const starts = (rel: string, ...prefixes: string[]) => prefixes.some((p) => rel === p || rel.startsWith(`${p}/`));

export const SECTIONS: Section[] = [
  {
    id: 'explorer',
    label: 'nav.explorer',
    icon: Database,
    to: '',
    links: [{ label: 'nav.redis', to: 'redis' }],
    owns: (rel) => rel === '' || starts(rel, 'db', 'redis'),
  },
  {
    id: 'query',
    label: 'nav.query',
    icon: SquareTerminal,
    to: 'q/',
    links: [],
    owns: (rel) => starts(rel, 'q'),
  },
  {
    id: 'saved',
    label: 'nav.saved',
    icon: Star,
    to: '',
    links: [],
    owns: () => false,
    panel: 'saved',
  },
  {
    id: 'history',
    label: 'nav.history',
    icon: History,
    to: '',
    links: [],
    owns: () => false,
    panel: 'history',
  },
  {
    id: 'dashboards',
    label: 'nav.dashboards',
    icon: LayoutDashboard,
    to: 'd',
    links: [],
    owns: (rel) => starts(rel, 'd'),
    side: 'dashboards',
    testId: 'nav-dashboards',
  },
  {
    id: 'messaging',
    label: 'nav.messaging',
    icon: Send,
    to: 'messaging',
    links: [],
    owns: (rel) => starts(rel, 'messaging'),
  },
  {
    id: 'logic',
    label: 'nav.logic',
    icon: SquareFunction,
    to: 'logic',
    links: [],
    owns: (rel) => starts(rel, 'logic'),
  },
  {
    id: 'performance',
    label: 'nav.performance',
    icon: Zap,
    to: 'indexing',
    links: [
      { label: 'nav.indexing', to: 'indexing' },
      { label: 'nav.inbox', to: 'inbox', end: true, badge: true },
      { label: 'nav.policy', to: 'inbox/policy' },
      { label: 'nav.shards', to: 'shards' },
    ],
    owns: (rel) => starts(rel, 'indexing', 'inbox', 'shards'),
  },
  {
    id: 'inheritance',
    label: 'nav.inheritance',
    icon: GitFork,
    to: 'inheritance',
    links: [],
    owns: (rel) => starts(rel, 'inheritance'),
  },
  {
    id: 'access',
    label: 'nav.accessControl',
    icon: Shield,
    to: 'access/users',
    links: [
      { label: 'nav.access', to: 'access/users' },
      { label: 'nav.policies', to: 'policies' },
      { label: 'nav.security', to: 'security' },
    ],
    owns: (rel) => starts(rel, 'access', 'policies', 'security'),
  },
  // Operations (backups, placement, diagnostics, external servers) and
  // Security (sign-on sources, encrypted fields and keys, the audit log,
  // residency) get their pages with their engine features. A section with
  // no pages isn't shown (hasPages); add each page's link here when it lands.
  {
    id: 'operations',
    label: 'nav.operations',
    icon: DataCentreIcon,
    to: null,
    links: [],
    owns: () => false,
  },
  {
    id: 'security',
    label: 'nav.securitySection',
    icon: ShieldCheck,
    to: null,
    links: [],
    owns: () => false,
  },
  {
    id: 'docs',
    label: 'nav.docs',
    icon: BookOpen,
    to: 'docs',
    links: [],
    owns: (rel) => starts(rel, 'docs'),
  },
  {
    id: 'operator',
    label: 'nav.operator',
    icon: ServerCog,
    to: '/ops/clusters',
    links: [],
    owns: () => false,
    bottom: true,
    operatorOnly: true,
    testId: 'nav-operator',
  },
  {
    id: 'settings',
    label: 'nav.settings',
    icon: Settings,
    to: 'settings',
    links: [
      { label: 'nav.settings', to: 'settings' },
      { label: 'nav.glossary', to: 'glossary' },
      { label: 'nav.digests', to: 'digests' },
    ],
    owns: (rel) => starts(rel, 'settings', 'glossary', 'digests'),
    bottom: true,
  },
];

/** The section a tenant-relative path belongs to (Explorer when none claims it). */
export function sectionOf(rel: string): Section {
  return SECTIONS.find((s) => s.owns(rel)) ?? SECTIONS[0]!;
}

/** Resolves a section or link target against the tenant base. */
export function resolve(base: string, to: string): string {
  return to.startsWith('/') ? to : to ? `${base}/${to}` : base;
}

/** Whether a section has anything to open: a page of its own, links, or a panel. */
export function hasPages(s: Section): boolean {
  return s.to !== null || s.links.length > 0 || !!s.panel || !!s.side;
}

/** Where a section's activity icon goes: its own page, else its first link. */
export function entryOf(s: Section): string {
  return s.to ?? s.links[0]?.to ?? '';
}

export function sectionById(id: SectionId): Section | undefined {
  return SECTIONS.find((s) => s.id === id);
}
