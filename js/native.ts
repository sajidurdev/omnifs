import { execSync } from "child_process";
import { readFileSync } from "fs";
import * as path from "path";

export type NativeBinding = {
  startDiscovery(root: string, options?: unknown): number;
  nextBatch(sessionId: number, maxItems?: number): Promise<any[] | null>;
  cancelDiscovery(sessionId: number): void;
  closeDiscovery(sessionId: number): void;
};

const PACKAGE_NAME = "omnifs";

function isFileMusl(file: string): boolean {
  return file.includes("libc.musl-") || file.includes("ld-musl-");
}

function isMuslFromFilesystem(): boolean | null {
  try {
    return readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {
    return null;
  }
}

function isMuslFromReport(): boolean | null {
  if (typeof process.report?.getReport !== "function") {
    return null;
  }

  (process.report as any).excludeNetwork = true;
  const report = process.report.getReport() as any;
  if (!report) {
    return null;
  }

  if (report.header && "glibcVersionRuntime" in report.header && report.header.glibcVersionRuntime) {
    return false;
  }

  if (Array.isArray(report.sharedObjects)) {
    return report.sharedObjects.some(isFileMusl);
  }

  return false;
}

function isMuslFromChildProcess(): boolean {
  try {
    return execSync("ldd --version", { encoding: "utf8" }).includes("musl");
  } catch {
    return false;
  }
}

function isMusl(): boolean {
  if (process.platform !== "linux") {
    return false;
  }

  const fsCheck = isMuslFromFilesystem();
  if (fsCheck !== null) {
    return fsCheck;
  }

  const reportCheck = isMuslFromReport();
  if (reportCheck !== null) {
    return reportCheck;
  }

  return isMuslFromChildProcess();
}

function tryRequire<T>(request: string, loadErrors: unknown[]): T | null {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require(request) as T;
  } catch (error) {
    loadErrors.push(error);
    return null;
  }
}

function loadFromLocalPaths(loadErrors: unknown[]): NativeBinding | null {
  const candidates = [
    path.resolve(__dirname, "..", "..", "rust-core", "index.node"),
    path.resolve(__dirname, "..", "..", "index.node"),
  ];

  for (let i = 0; i < candidates.length; i += 1) {
    const binding = tryRequire<NativeBinding>(candidates[i], loadErrors);
    if (binding) {
      return binding;
    }
  }

  return null;
}

function loadFromPublishedPackage(loadErrors: unknown[]): NativeBinding | null {
  const tuple = (() => {
    if (process.platform === "linux") {
      if (process.arch === "x64") {
        return isMusl() ? "linux-x64-musl" : "linux-x64-gnu";
      }
      if (process.arch === "arm64") {
        return isMusl() ? "linux-arm64-musl" : "linux-arm64-gnu";
      }
      return null;
    }

    if (process.platform === "darwin") {
      if (process.arch === "x64") {
        return "darwin-x64";
      }
      if (process.arch === "arm64") {
        return "darwin-arm64";
      }
      return null;
    }

    if (process.platform === "win32") {
      if (process.arch === "x64") {
        return "win32-x64-msvc";
      }
      if (process.arch === "arm64") {
        return "win32-arm64-msvc";
      }
      return null;
    }

    return null;
  })();

  if (!tuple) {
    loadErrors.push(new Error(`Unsupported platform: ${process.platform}/${process.arch}`));
    return null;
  }

  return tryRequire<NativeBinding>(`${PACKAGE_NAME}-${tuple}`, loadErrors);
}

export function loadNativeBinding(): NativeBinding {
  const loadErrors: unknown[] = [];

  if (process.env.NAPI_RS_NATIVE_LIBRARY_PATH) {
    const fromEnv = tryRequire<NativeBinding>(process.env.NAPI_RS_NATIVE_LIBRARY_PATH, loadErrors);
    if (fromEnv) {
      return fromEnv;
    }
  }

  const localBinding = loadFromLocalPaths(loadErrors);
  if (localBinding) {
    return localBinding;
  }

  const packageBinding = loadFromPublishedPackage(loadErrors);
  if (packageBinding) {
    return packageBinding;
  }

  throw new Error(
    "Unable to load omnifs native binding. Reinstall dependencies and verify optional platform package installation.",
    {
      cause: loadErrors[loadErrors.length - 1],
    },
  );
}
