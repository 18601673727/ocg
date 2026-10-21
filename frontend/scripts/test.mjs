import ts from "typescript";
import { mkdirSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const tests = readdirSync(join(root, "tests")).filter((name) => /(?:\.test|\.type-test)\.ts$/.test(name));
mkdirSync(join(root, ".ocg"), { recursive: true });
const output = mkdtempSync(join(root, ".ocg", "tests-"));
try {
  const config = ts.readConfigFile(join(root, "tsconfig.json"), ts.sys.readFile);
  if (config.error) throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"));
  const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root, {
    noEmit: false,
    noEmitOnError: true,
    incremental: false,
    module: ts.ModuleKind.CommonJS,
    moduleResolution: ts.ModuleResolutionKind.Node10,
    rootDir: root,
    outDir: output,
  });
  const program = ts.createProgram([
    ...tests.map((name) => join(root, "tests", name)),
    ...parsed.fileNames.filter((name) => name.endsWith(".d.ts")),
  ], parsed.options);
  // TypeScript resolves the app alias but does not rewrite it for Node.
  const aliases = (context) => {
    const visit = (node) => {
      if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier) && node.moduleSpecifier.text.startsWith("@/")) {
        return ts.factory.updateImportDeclaration(node, node.modifiers, node.importClause,
          ts.factory.createStringLiteral(join(output, node.moduleSpecifier.text.slice(2))), node.attributes);
      }
      return ts.visitEachChild(node, visit, context);
    };
    return (source) => ts.visitNode(source, visit);
  };
  const emitted = program.emit(undefined, undefined, undefined, undefined, { before: [aliases] });
  const diagnostics = [...parsed.errors, ...ts.getPreEmitDiagnostics(program), ...emitted.diagnostics];
  if (diagnostics.length) {
    process.stderr.write(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCurrentDirectory: () => root, getCanonicalFileName: (name) => name, getNewLine: () => "\n",
    }));
    process.exitCode = 1;
  } else {
    const result = spawnSync(process.execPath, ["--test", ...tests.filter((name) => name.endsWith(".test.ts"))
      .map((name) => join(output, "tests", name.replace(/\.ts$/, ".js")))], { stdio: "inherit" });
    if (result.error) throw result.error;
    process.exitCode = result.status ?? 1;
  }
} finally {
  rmSync(output, { recursive: true, force: true });
}
