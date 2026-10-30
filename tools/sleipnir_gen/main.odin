// =============================================================================
// Sleipnir AST Metaprogramming CLI Generator (`tools/sleipnir_gen`)
//
// Scans an Odin package directory for `@(actor)` structs and `@(behavior)`
// procedures using `core:odin/parser` and emits `<dir>/actors_gen.odin`.
//
// Usage:
//   odin run tools/sleipnir_gen -- <target_package_directory>
// =============================================================================

package main

import codegen "../../actor/codegen"
import "core:fmt"
import "core:os"

main :: proc() {
	if len(os.args) < 2 {
		fmt.eprintln("Usage: odin run tools/sleipnir_gen -- <package_dir>")
		os.exit(1)
	}

	target_dir := os.args[1]
	out_path, ok := codegen.generate_directory(target_dir)
	if !ok {
		fmt.eprintf("Error: failed to generate actor bindings in '%s'\n", target_dir)
		os.exit(1)
	}
	defer delete(out_path)
	fmt.printfln("Generated Sleipnir actor bindings -> %s", out_path)
}
