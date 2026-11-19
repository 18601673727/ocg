import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { defineConfig, type RsbuildPlugin } from "@rsbuild/core";
import { pluginSolid } from "@rsbuild/plugin-solid";
import { THEME_BOOTSTRAP_SCRIPT } from "./components/ocg/appearance/theme-bootstrap";
import { LOCALE_BOOTSTRAP_SCRIPT } from "./components/ocg/i18n/locale-bootstrap";
import { SETTINGS_SECTIONS, settingsPath } from "./components/ocg/settings/settings-sections";

const htmlAliases: RsbuildPlugin = {
  name: "ocg:html-aliases",
  setup(api) {
    api.onAfterBuild(async ({ environments, stats }) => {
      if (stats?.hasErrors()) return;
      const { distPath } = environments.web;
      const index = await readFile(join(distPath, "index.html"), "utf8");
      const routes = [
        "health",
        "jobs",
        "login",
        "logs",
        "usage",
        "settings",
        ...SETTINGS_SECTIONS.map(({ id }) => settingsPath(id).slice(1)),
      ];
      await Promise.all(
        routes.map(async (route) => {
          const destination = join(distPath, `${route}.html`);
          await mkdir(dirname(destination), { recursive: true });
          const page =
            route === "logs"
              ? "Logs"
              : route === "settings" || route.startsWith("settings/")
                ? "Settings"
                : null;
          const title = page ? `${page} - OCG Workspace` : "OCG Workspace";
          await writeFile(
            destination,
            index.replace("<title>Home - OCG Workspace</title>", `<title>${title}</title>`),
          );
        }),
      );
    });
  },
};

export default defineConfig({
  html: {
    template: "./index.html",
    tags: [
      { tag: "script", children: THEME_BOOTSTRAP_SCRIPT, head: true, append: false },
      { tag: "script", children: LOCALE_BOOTSTRAP_SCRIPT, head: true, append: false },
    ],
  },
  output: {
    assetPrefix: "/",
    distPath: {
      root: "out",
      js: "_assets/js",
      jsAsync: "_assets/js/async",
      css: "_assets/css",
      cssAsync: "_assets/css/async",
      font: "_assets/font",
      svg: "_assets/svg",
      image: "_assets/image",
      assets: "_assets/files",
      wasm: "_assets/wasm",
      media: "_assets/media",
    },
  },
  plugins: [pluginSolid(), htmlAliases],
});
