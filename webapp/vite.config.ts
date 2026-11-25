import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base './' lets the built site work from any path (GitHub Pages project site, vite preview, a file server)
export default defineConfig({
  base: "./",
  plugins: [react()],
  test: { environment: "node" },
});
