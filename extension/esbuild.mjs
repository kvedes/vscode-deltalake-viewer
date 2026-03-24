import * as esbuild from "esbuild";

const watch = process.argv.includes("--watch");

/** @type {esbuild.BuildOptions} */
const extensionConfig = {
  entryPoints: ["src/extension.ts"],
  bundle: true,
  outfile: "out/extension.js",
  external: ["vscode"],
  format: "cjs",
  platform: "node",
  target: "node20",
  sourcemap: true,
};

/** @type {esbuild.BuildOptions} */
const webviewConfig = {
  entryPoints: ["webview-ui/main.ts"],
  bundle: true,
  outfile: "out/webview.js",
  format: "iife",
  platform: "browser",
  target: "es2022",
  sourcemap: true,
};

if (watch) {
  const ctx1 = await esbuild.context(extensionConfig);
  const ctx2 = await esbuild.context(webviewConfig);
  await Promise.all([ctx1.watch(), ctx2.watch()]);
  console.log("Watching...");
} else {
  await esbuild.build(extensionConfig);
  await esbuild.build(webviewConfig);
  console.log("Build complete.");
}
