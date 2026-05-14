import { describe, it, expect } from "vitest";
import * as fs from "fs";
import * as path from "path";

// -------------------------------------------------------------------------
// Failing test for the bug documented in /review.md §1.5.
// -------------------------------------------------------------------------

/// Returns the body of the arrow function passed as the second argument to
/// `registerCommand("deltaViewer.<name>", ...)`. Naive but sufficient for the
/// small surface of `commands.ts` — it locates the command id and then walks
/// braces until the function body is balanced.
function extractHandlerBody(source: string, commandId: string): string {
  const marker = `"${commandId}"`;
  const start = source.indexOf(marker);
  if (start < 0) {
    throw new Error(`command id ${commandId} not found in source`);
  }
  // Find the first '{' after the marker — that opens the async-arrow body.
  const arrow = source.indexOf("=> {", start);
  if (arrow < 0) throw new Error(`arrow body not found for ${commandId}`);
  let i = arrow + 3; // index of the opening '{'
  let depth = 1;
  i++;
  while (i < source.length && depth > 0) {
    const ch = source[i];
    if (ch === "{") depth++;
    else if (ch === "}") depth--;
    i++;
  }
  return source.slice(arrow + 4, i - 1).trim();
}

describe("commands.ts", () => {
  // review.md §1.5 — `openParquet` and `openRawParquet` are two separately
  // registered commands but their handlers are byte-identical, so the
  // user-facing "Open as Raw Parquet" title is dead UX. Either the "raw"
  // command should bypass delta-root detection (the user-visible intent),
  // or it should be deleted as a duplicate.
  it("bug_1_5 openRawParquet handler must not be a verbatim duplicate of openParquet", () => {
    const src = fs.readFileSync(
      path.join(__dirname, "..", "commands.ts"),
      "utf-8",
    );
    const plain = extractHandlerBody(src, "deltaViewer.openParquet");
    const raw = extractHandlerBody(src, "deltaViewer.openRawParquet");

    expect(raw).not.toBe(plain);
  });
});
