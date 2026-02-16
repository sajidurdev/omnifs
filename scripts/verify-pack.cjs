const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");

const cacheDir = path.resolve(__dirname, "..", ".npm-cache-verify");
fs.mkdirSync(cacheDir, { recursive: true });
const args = ["pack", "--dry-run", "--json", "--ignore-scripts", "--cache", cacheDir];
const output =
  process.platform === "win32"
    ? execFileSync(process.env.ComSpec || "cmd.exe", ["/d", "/s", "/c", `npm ${args.join(" ")}`], {
        encoding: "utf8",
      })
    : execFileSync("npm", args, { encoding: "utf8" });

let parsed;
try {
  parsed = JSON.parse(output);
} catch (err) {
  console.error("[verify:pack] failed to parse npm pack output");
  console.error(output);
  throw err;
}

if (!Array.isArray(parsed) || parsed.length === 0 || !Array.isArray(parsed[0].files)) {
  console.error("[verify:pack] unexpected npm pack output format");
  process.exit(1);
}

const files = parsed[0].files.map((entry) => entry.path);
const allowedExact = new Set(["LICENSE", "README.md", "package.json"]);

function isAllowed(path) {
  if (allowedExact.has(path)) {
    return true;
  }
  if (path.startsWith("dist/")) {
    return true;
  }
  return false;
}

const disallowed = files.filter((path) => !isAllowed(path));
if (disallowed.length > 0) {
  console.error("[verify:pack] disallowed files detected in npm package:");
  for (const path of disallowed) {
    console.error(`  - ${path}`);
  }
  process.exit(1);
}

console.log(`[verify:pack] ok (${files.length} files)`);
