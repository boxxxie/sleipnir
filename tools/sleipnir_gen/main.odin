// =============================================================================
// Sleipnir AST Metaprogramming CLI Generator (`tools/sleipnir_gen`)
//
// Scans one or more Odin package directories for `@(actor)` structs and
// `@(behavior)` procedures across all `.odin` files in each directory using
// `core:odin/parser` and emits `<dir>/actors_gen.odin`.
//
// Usage:
//   odin run tools/sleipnir_gen -- <package_dir> [<package_dir2> ...]
// =============================================================================

package main

import codegen "../../actor/codegen"
import "core:fmt"
import "core:os"

main :: proc() {
	if len(os.args) < 2 {
		fmt.eprintln("Usage: odin run tools/sleipnir_gen -- <package_dir> [<package_dir2> ...]")
		os.exit(1)
	}

	for target_dir in os.args[1:] {
		out_path, ok := codegen.generate_directory(target_dir)
		if !ok {
			fmt.eprintf("Error: failed to generate actor bindings in '%s'\n", target_dir)
			os.exit(1)
		}
		fmt.printfln("Generated Sleipnir actor bindings -> %s", out_path)
		delete(out_path)
	}
}
