import { defineConfig } from "bumpp";
import { globSync } from "tinyglobby";

export default defineConfig({
	// `files` replaces bumpp's default of ["package.json"], so the root has to be
	// listed explicitly or it drifts off the shared version line on every bump.
	files: [
		"package.json",
		...globSync(["./packages/*/package.json"], { expandDirectories: false }),
	],
});
