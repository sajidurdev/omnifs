const fs = require("fs");
const path = require("path");

const outFile = path.resolve(__dirname, "..", "dist", "esm", "index.mjs");
const outDir = path.dirname(outFile);

const source = `import cjs from "../js/index.js";

export const discover = cjs.discover;
export const discoverBatched = cjs.discoverBatched;
`;

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(outFile, source, "utf8");
