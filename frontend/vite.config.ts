import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [vue()],
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  build: {
    outDir: resolve(import.meta.dirname, "../tateros_static/ui"),
    emptyOutDir: true,
    lib: {
      entry: resolve(import.meta.dirname, "src/entry.ts"),
      formats: ["es"],
      fileName: () => "tater-ui.js",
    },
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        assetFileNames: (assetInfo) =>
          assetInfo.name?.endsWith(".css") ? "tater-ui.css" : "[name][extname]",
      },
    },
  },
});
