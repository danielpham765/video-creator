export type RuntimeConfigListener = (payload: {
  version: number;
  loadedAt: string;
  changes: string[];
}) => void;

export interface RuntimeConfigSnapshot {
  globalDefault: Record<string, any>;
  sourceOverrides: Record<string, Record<string, any>>;
  effectiveBySource: Record<string, Record<string, any>>;
  cookiesBySource: Record<string, string>;
  version: number;
  loadedAt: string;
  sourceFileMeta: Record<string, { mtimeMs: number; size: number }>;
}

export interface RuntimeReloadStatus {
  version: number;
  loadedAt: string;
  error?: string;
  watchedFiles: string[];
  pendingRestartKeys: string[];
}
