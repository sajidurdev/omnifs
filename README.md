# <p align="center">omnifs</p>

<p align="center">
  <b>High-performance filesystem discovery engine for Node.js.</b><br>
  <i>Designed for build tools, search indexers, and large monorepo workflows.</i>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/omnifs"><img src="https://img.shields.io/npm/v/omnifs?style=flat-square&color=CB3837" alt="NPM Version"></a>
  <a href="https://github.com/sajidurdev/omnifs/blob/main/LICENSE"><img src="https://img.shields.io/npm/l/omnifs?style=flat-square&color=blue" alt="License"></a>
  <a href="https://nodejs.org"><img src="https://img.shields.io/badge/node-%3E%3D20-green?style=flat-square&logo=node.js" alt="Node.js Support"></a>
  <a href="https://www.typescriptlang.org/"><img src="https://img.shields.io/badge/TypeScript-Ready-blue?style=flat-square&logo=typescript" alt="TypeScript Ready"></a>
</p>

---

## 🏗️ Architecture & Intent

`omnifs` is a low-level filesystem engine designed to bridge the performance gap between Node.js and native system calls. By offloading recursive traversal, glob filtering, and hashing to a multi-threaded **Rust core** via `napi-rs`, it reduces pressure on the Node.js event loop during large traversal workloads and minimizes garbage collection overhead.

### Key Capabilities
* **Parallel Traversal:** Adaptive worker pooling to saturate available I/O bandwidth.
* **Ignore Semantics:** Native support for recursive `.gitignore` hierarchy.
* **Incremental Mode:** JSONL-backed indexing to skip unchanged subtrees in subsequent scans.
* **Streaming API:** Async Iterator interface with backpressure-aware batching.
* **Integrity Pipeline:** Optional `blake3` hashing integrated directly into traversal.

---

## ⚖️ Capability Comparison

| Capability | Path Walkers | Glob Utilities | **omnifs** |
| :--- | :---: | :---: | :---: |
| **Rust-Powered Engine** | ❌ | ❌ | ✅ |
| **Async Streaming API** | ⚠️ Partial | ✅ | ✅ |
| **Ignore-aware Traversal** | ⚠️ Limited | ⚠️ Pattern-only | ✅ |
| **Incremental Rescans** | ❌ | ❌ | ✅ |
| **Native Hashing Pipeline** | ❌ | ❌ | ✅ |
| **Deterministic Ordering** | ❌ | ❌ | ✅ |

---

## 📊 Performance Characteristics

`omnifs` is optimized for balanced throughput, low latency streaming, and stable memory usage.  
Observed ranges below were measured on a **~100,000 file dataset**.

> Performance varies depending on filesystem type, dataset structure, CPU, and storage medium.

* **Ignore-aware Traversal:** ~600–700ms total runtime.
* **Latency to First Result:** ~2–5ms.
* **Memory Footprint:** ~120–180MB peak RSS under sustained load.
* **Incremental Rescan:** ~80ms via metadata-based subtree skipping.
* **Hashing (`blake3`):** ~4–6s for full-content hashing across 100k files.

---

## 🚀 Usage

### Installation

`omnifs` ships a JS wrapper plus platform-specific native binaries selected automatically at install/runtime.

```bash
npm install omnifs
```

> Native binaries load lazily at runtime. No build step is required on supported platforms.

---

### ⚠️ Troubleshooting (Linux CI / Docker)

Some Linux CI environments skip optional native dependencies by default.
If the native engine fails to load:

```bash
npm install --include=optional omnifs
```

If npm fails to recover optional dependencies due to lockfile state:

```bash
rm -rf node_modules package-lock.json
npm install
```

This resolves known npm optional-dependency edge cases in strict CI setups.

> If you're using pnpm, ensure `pnpm` is not configured to ignore optional dependencies.

---

### Basic Discovery

Returns an `AsyncGenerator<FileMetadata>`.

```ts
import { discover } from "omnifs";

for await (const file of discover("./src")) {
  console.log(`${file.path} — ${file.size} bytes`);
}
```

---

### Batched Processing

```ts
import { discoverBatched } from "omnifs";

for await (const batch of discoverBatched(".", { batchSize: 512 })) {
  await Promise.all(batch.map(file => process(file)));
}
```

---

## 📦 Module Compatibility

`omnifs` works across modern Node.js module systems.

### ESM

```ts
import { discover } from "omnifs";
```

### CommonJS

```js
const { discover } = require("omnifs");
```

### TypeScript

```ts
import type { FileMetadata } from "omnifs";
```

---

## 📄 File Metadata

Each emitted entry contains:

* `path: string`
* `size: number`
* `mtimeMs: number`
* `identity: string`
* `hash?: string | null`

---

## ⚙️ Configuration Options

| Option             | Type                                      | Default     | Behavior                                  |
| :----------------- | :---------------------------------------- | :---------- | :---------------------------------------- |
| `patterns`         | `string[]`                                | `[]`        | Glob include patterns.                    |
| `respectGitignore` | `boolean`                                 | `true`      | Honors `.gitignore` rules.                |
| `incremental`      | `boolean`                                 | `false`     | Enables change detection.                 |
| `hash`             | `"blake3" \| false`                       | `false`     | Enables hashing pipeline.                 |
| `mode`             | `"auto" \| "crawl" \| "glob" \| "ignore"` | `"auto"`    | Traversal strategy.                       |
| `fastPath`         | `"auto" \| "glob" \| "none"`              | `"auto"`    | Enables glob fast-path.                   |
| `fingerprinting`   | `boolean`                                 | `false*`    | Skip unchanged subtrees.                  |
| `forceHash`        | `boolean`                                 | `false`     | Hash unchanged files in incremental mode. |
| `deterministic`    | `boolean`                                 | `false`     | Stable ordering.                          |
| `batchSize`        | `number`                                  | `256`       | Adaptive batch size.                      |
| `threads`          | `number`                                  | `CPUs`      | Worker count.                             |
| `signal`           | `AbortSignal`                             | `undefined` | Cancellation support.                     |

---

## 🧩 Logical Semantics

### Modes

* `auto` — Default optimized path.
* `crawl` — Full traversal.
* `glob` — Pattern-focused mode.
* `ignore` — Exclusion-focused traversal.

### Hash & Incremental Rules

* Incremental + No Hash → Metadata comparison only.
* Incremental + blake3 → Hash changed files.
* Standard + blake3 → Hash all emitted files.

---

## ⚠️ When NOT to use omnifs

If you only need simple glob expansion without metadata or streaming guarantees, lightweight JS glob libraries may be faster.

---

## 🛠️ Requirements & Platform Support

* Node.js `>=20`
* ESM / CommonJS / TypeScript supported
* Linux: x64/arm64 (gnu + musl)
* macOS: x64/arm64
* Windows: x64/arm64

---

## 📄 License

MIT — Copyright © 2026

<p align="center">
  <br>
  <b>omnifs</b> • 2026
</p>