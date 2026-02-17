const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const NPM_DIR = path.join(ROOT, "npm");
const SCOPE = "@omnifs";
const TARGET_ORDER = [
  "linux-x64-gnu",
  "linux-x64-musl",
  "linux-arm64-gnu",
  "linux-arm64-musl",
  "darwin-x64",
  "darwin-arm64",
  "win32-x64-msvc",
  "win32-arm64-msvc",
];

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeJson(file, value) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function getTargets() {
  if (!fs.existsSync(NPM_DIR)) {
    return [];
  }

  const dirs = fs
    .readdirSync(NPM_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name);

  const ordered = TARGET_ORDER.filter((target) => dirs.includes(target));
  const extras = dirs.filter((target) => !TARGET_ORDER.includes(target)).sort();
  return [...ordered, ...extras];
}

function normalizeTargetPackage(target) {
  const pkgPath = path.join(NPM_DIR, target, "package.json");
  if (!fs.existsSync(pkgPath)) {
    return;
  }

  const pkg = readJson(pkgPath);
  pkg.name = `${SCOPE}/${target}`;
  writeJson(pkgPath, pkg);

  const readmePath = path.join(NPM_DIR, target, "README.md");
  if (fs.existsSync(readmePath)) {
    const current = fs.readFileSync(readmePath, "utf8");
    const triple = (current.match(/\*\*(.+?)\*\*/) || [])[1] ?? target;
    const next = `# \`${SCOPE}/${target}\`\n\nThis is the **${triple}** binary for \`omnifs\`\n`;
    fs.writeFileSync(readmePath, next, "utf8");
  }
}

function normalizeRootOptionalDependencies(targets) {
  const rootPkgPath = path.join(ROOT, "package.json");
  const rootPkg = readJson(rootPkgPath);

  const optionalDependencies = {};
  for (const target of targets) {
    optionalDependencies[`${SCOPE}/${target}`] = rootPkg.version;
  }

  rootPkg.optionalDependencies = optionalDependencies;
  writeJson(rootPkgPath, rootPkg);
}

function main() {
  const targets = getTargets();
  for (const target of targets) {
    normalizeTargetPackage(target);
  }
  normalizeRootOptionalDependencies(targets);
  console.log(`Scoped ${targets.length} platform package(s) to ${SCOPE}/<target>.`);
}

main();
