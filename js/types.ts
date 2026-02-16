export type HashMode = "blake3" | false;
export type DiscoverMode = "auto" | "crawl" | "glob" | "ignore";

export interface DiscoverOptions {
  patterns?: string[];
  respectGitignore?: boolean;
  incremental?: boolean;
  hash?: HashMode;
  mode?: DiscoverMode;
  fastPath?: "auto" | "glob" | "none";
  fingerprinting?: boolean;
  forceHash?: boolean;
  deterministic?: boolean;
  batchSize?: number;
  threads?: number;
  signal?: AbortSignal;
}

export interface FileMetadata {
  path: string;
  size: number;
  mtimeMs: number;
  identity: string;
  hash?: string | null;
}
