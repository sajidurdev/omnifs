import type { DiscoverOptions, FileMetadata } from "./types";
import { performance } from "perf_hooks";
import { loadNativeBinding, type NativeBinding } from "./native";

type NativeOptions = {
  patterns?: string[];
  respect_gitignore?: boolean;
  incremental?: boolean;
  hash?: string;
  mode?: "auto" | "crawl" | "glob" | "ignore";
  fast_path?: "auto" | "glob" | "none";
  fingerprinting?: boolean;
  force_hash?: boolean;
  deterministic?: boolean;
  batch_size?: number;
  threads?: number;
};

type NativeFileMetadata = {
  path: string;
  size: number;
  mtime_ms?: number;
  mtimeMs?: number;
  identity: string;
  hash?: string | null;
};

const native = loadNativeBinding() as NativeBinding;
const MIN_PULL_SIZE = 64;
const MAX_PULL_SIZE = 8192;

function clampPullSize(value: number): number {
  return Math.min(MAX_PULL_SIZE, Math.max(MIN_PULL_SIZE, Math.floor(value)));
}

function nextAdaptivePullSize(
  current: number,
  batchLength: number,
  pullWaitMs: number,
  consumeMs: number,
): number {
  let next = current;
  if (batchLength >= current * 0.9) {
    next = current * 2;
  } else if (batchLength <= current / 4 && pullWaitMs > 8) {
    next = current / 2;
  }

  // Slow consumer cadence benefits from fewer JS<->native crossings.
  if (consumeMs > 8 && batchLength >= current / 2) {
    next = Math.max(next, current * 2);
  } else if (consumeMs < 2 && batchLength < current / 3) {
    next = Math.min(next, current / 2);
  }

  return clampPullSize(next);
}

function normalizeBatchInPlace(batch: NativeFileMetadata[]): FileMetadata[] {
  for (let i = 0; i < batch.length; i += 1) {
    const file = batch[i] as NativeFileMetadata & FileMetadata;
    if (file.mtimeMs === undefined) {
      file.mtimeMs = file.mtime_ms ?? 0;
    }
    if (file.hash === null) {
      file.hash = undefined;
    }
  }
  return batch as unknown as FileMetadata[];
}

function toNativeOptions(options: DiscoverOptions): NativeOptions {
  const throughputMode = options.incremental !== true && options.hash !== "blake3";
  const nativeBatchHint =
    options.batchSize === undefined
      ? undefined
      : throughputMode
        ? Math.max(256, options.batchSize)
        : options.batchSize;
  return {
    patterns: options.patterns,
    respect_gitignore: options.respectGitignore,
    incremental: options.incremental,
    hash: options.hash === "blake3" ? "blake3" : undefined,
    mode: options.mode,
    fast_path: options.fastPath,
    fingerprinting: options.fingerprinting,
    force_hash: options.forceHash,
    deterministic: options.deterministic,
    batch_size: nativeBatchHint,
    threads: options.threads,
  };
}

export async function* discover(root: string, options: DiscoverOptions = {}): AsyncGenerator<FileMetadata> {
  const nativeOptions = toNativeOptions(options);

  const sessionId = native.startDiscovery(root, nativeOptions);
  const onAbort = () => native.cancelDiscovery(sessionId);

  if (options.signal) {
    if (options.signal.aborted) {
      onAbort();
    }
    options.signal.addEventListener("abort", onAbort, { once: true });
  }

  let nextBatchPromise: Promise<NativeFileMetadata[] | null> | null = null;
  try {
    let pullSize = clampPullSize(options.batchSize ?? 256);
    nextBatchPromise = native.nextBatch(sessionId, pullSize);
    while (true) {
      const pullStartedAt = performance.now();
      const batch = await nextBatchPromise;
      const pullWaitMs = performance.now() - pullStartedAt;
      if (!batch || batch.length === 0) {
        nextBatchPromise = null;
        break;
      }
      const consumeStartedAt = performance.now();
      nextBatchPromise = native.nextBatch(sessionId, pullSize);
      const normalized = normalizeBatchInPlace(batch);
      for (let i = 0; i < normalized.length; i += 1) {
        yield normalized[i];
      }
      const consumeMs = performance.now() - consumeStartedAt;
      pullSize = nextAdaptivePullSize(pullSize, batch.length, pullWaitMs, consumeMs);
    }
  } finally {
    if (nextBatchPromise) {
      void nextBatchPromise.catch(() => undefined);
    }
    if (options.signal) {
      options.signal.removeEventListener("abort", onAbort);
    }
    native.closeDiscovery(sessionId);
  }
}

export async function* discoverBatched(
  root: string,
  options: DiscoverOptions = {},
): AsyncGenerator<FileMetadata[]> {
  const sessionId = native.startDiscovery(root, toNativeOptions(options));
  const onAbort = () => native.cancelDiscovery(sessionId);

  if (options.signal) {
    if (options.signal.aborted) {
      onAbort();
    }
    options.signal.addEventListener("abort", onAbort, { once: true });
  }

  let nextBatchPromise: Promise<NativeFileMetadata[] | null> | null = null;
  try {
    let pullSize = clampPullSize(options.batchSize ?? 256);
    nextBatchPromise = native.nextBatch(sessionId, pullSize);
    while (true) {
      const pullStartedAt = performance.now();
      const batch = await nextBatchPromise;
      const pullWaitMs = performance.now() - pullStartedAt;
      if (!batch || batch.length === 0) {
        nextBatchPromise = null;
        break;
      }
      const consumeStartedAt = performance.now();
      nextBatchPromise = native.nextBatch(sessionId, pullSize);
      yield normalizeBatchInPlace(batch);
      const consumeMs = performance.now() - consumeStartedAt;
      pullSize = nextAdaptivePullSize(pullSize, batch.length, pullWaitMs, consumeMs);
    }
  } finally {
    if (nextBatchPromise) {
      void nextBatchPromise.catch(() => undefined);
    }
    if (options.signal) {
      options.signal.removeEventListener("abort", onAbort);
    }
    native.closeDiscovery(sessionId);
  }
}

export type { DiscoverOptions, FileMetadata } from "./types";
