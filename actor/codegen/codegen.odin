package actor_codegen

import "core:fmt"
import "core:mem"
import "core:odin/ast"
import "core:odin/parser"
import "core:odin/tokenizer"
import "core:os"
import "core:path/filepath"
import "core:slice"
import "core:strings"

// =============================================================================
// Sleipnir AST Metaprogramming Generator (`core:odin/parser` + `core:odin/ast`)
//
// Parses Odin source files containing `@(actor)` structs and `@(behavior)`
// procedures and generates strongly-typed message structs, tagged `union`
// mailboxes, `PID` aliases, dispatchers, `spawn_<actor>` constructors, and
// Pony-style `be_<behavior>` / `send_<behavior>` async message senders.
// =============================================================================

Behavior_Param :: struct {
	name:         string,
	type_expr:    string,
	default_expr: string,
}

Behavior_Spec :: struct {
	proc_name:  string,
	actor_type: string,
	takes_ctx:  bool,
	params:     [dynamic]Behavior_Param,
}

Actor_Spec :: struct {
	name:      string,
	behaviors: [dynamic]Behavior_Spec,
}

Module_Spec :: struct {
	package_name: string,
	actor_import: string,
	actors:       [dynamic]Actor_Spec,
	ast_arena:    mem.Dynamic_Arena,
}

init_module_spec :: proc(m: ^Module_Spec) {
	m.actors = make([dynamic]Actor_Spec)
	mem.dynamic_arena_init(&m.ast_arena)
}

destroy_module_spec :: proc(m: ^Module_Spec) {
	for &a in m.actors {
		for &b in a.behaviors {
			delete(b.params)
		}
		delete(a.behaviors)
	}
	delete(m.actors)
	mem.dynamic_arena_destroy(&m.ast_arena)
}

// Convert `Pascal_Case` or `CamelCase` identifier to `snake_case`.
to_snake_case :: proc(s: string, allocator := context.allocator) -> string {
	b := strings.builder_make(allocator)
	for i := 0; i < len(s); i += 1 {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' {
			if i > 0 && s[i - 1] != '_' && !(s[i - 1] >= 'A' && s[i - 1] <= 'Z') {
				strings.write_byte(&b, '_')
			}
			strings.write_byte(&b, ch + ('a' - 'A'))
		} else {
			strings.write_byte(&b, ch)
		}
	}
	return strings.to_string(b)
}

// Check if a `Value_Decl` has `@(attr_name)` or `@attr_name`.
has_attribute :: proc(decl: ^ast.Value_Decl, attr_name: string) -> bool {
	for attr in decl.attributes {
		for elem in attr.elems {
			#partial switch e in elem.derived {
			case ^ast.Ident:
				if e.name == attr_name do return true
			case ^ast.Field_Value:
				if ident, ok := e.field.derived.(^ast.Ident); ok {
					if ident.name == attr_name do return true
				}
			}
		}
	}
	return false
}

// Extract the raw source substring corresponding to an AST node.
node_source :: proc(src: string, node: ^ast.Node) -> string {
	if node == nil do return ""
	start := node.pos.offset
	end   := node.end.offset
	if start >= 0 && end >= start && end <= len(src) {
		return strings.trim_space(src[start:end])
	}
	return ""
}

// Extract the pointed-to struct name if `expr` is `^Actor_Name`.
extract_pointer_ident :: proc(expr: ^ast.Expr) -> (string, bool) {
	if expr == nil do return "", false
	if ptr, ok := expr.derived.(^ast.Pointer_Type); ok {
		if ident, ident_ok := ptr.elem.derived.(^ast.Ident); ident_ok {
			return ident.name, true
		}
		if sel, sel_ok := ptr.elem.derived.(^ast.Selector_Expr); sel_ok {
			if field_ident, f_ok := sel.field.derived.(^ast.Ident); f_ok {
				return field_ident.name, true
			}
		}
	}
	return "", false
}

// Parse a single Odin source buffer and append discovered `@(actor)` structs and
// `@(behavior)` procedures into `mod` and `pending_behaviors`.
parse_source_into_spec :: proc(
	src: string,
	filename: string,
	mod: ^Module_Spec,
	pending_behaviors: ^[dynamic]Behavior_Spec,
) -> bool {
	arena_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)
	p := parser.default_parser()
	file: ^ast.File
	{
		context.allocator = arena_alloc
		file = ast.new(ast.File, tokenizer.Pos{}, tokenizer.Pos{})
		file.src      = src
		file.fullpath = filename
		if !parser.parse_file(&p, file) || p.error_count > 0 {
			return false
		}
	}

	if len(mod.package_name) == 0 && file.pkg_name != "" {
		mod.package_name = file.pkg_name
	}

	for decl_stmt in file.decls {
		#partial switch d in decl_stmt.derived {
		case ^ast.Import_Decl:
			if strings.has_suffix(d.fullpath, "actor\"") || strings.has_suffix(d.fullpath, "/actor\"") {
				mod.actor_import = d.fullpath
			}
		case ^ast.Value_Decl:
			if d.is_mutable || len(d.names) == 0 || len(d.values) == 0 {
				continue
			}
			ident, ok := d.names[0].derived.(^ast.Ident)
			if !ok do continue

			if has_attribute(d, "actor") {
				if _, is_struct := d.values[0].derived.(^ast.Struct_Type); is_struct {
					already_exists := false
					for existing in mod.actors {
						if existing.name == ident.name {
							already_exists = true
							break
						}
					}
					if !already_exists {
						append(&mod.actors, Actor_Spec{
							name      = ident.name,
							behaviors = make([dynamic]Behavior_Spec),
						})
					}
				}
			} else if has_attribute(d, "behavior") {
				proc_lit, is_proc := d.values[0].derived.(^ast.Proc_Lit)
				if !is_proc || proc_lit.type == nil || proc_lit.type.params == nil {
					continue
				}
				fields := proc_lit.type.params.list
				if len(fields) == 0 do continue

				// Determine whether param 0 is `self: ^Actor` or `(ctx: ^Context, self: ^Actor)`
				actor_type := ""
				takes_ctx  := false
				start_field_idx := 1

				if first_ptr, ok0 := extract_pointer_ident(fields[0].type); ok0 {
					if first_ptr == "Context" && len(fields) >= 2 {
						if second_ptr, ok1 := extract_pointer_ident(fields[1].type); ok1 {
							actor_type = second_ptr
							takes_ctx  = true
							start_field_idx = 2
						}
					} else {
						actor_type = first_ptr
						takes_ctx  = false
						start_field_idx = 1
					}
				}

				if actor_type == "" do continue

				bspec := Behavior_Spec{
					proc_name  = ident.name,
					actor_type = actor_type,
					takes_ctx  = takes_ctx,
					params     = make([dynamic]Behavior_Param),
				}

				for f_idx := start_field_idx; f_idx < len(fields); f_idx += 1 {
					field := fields[f_idx]
					type_str := node_source(src, field.type)
					def_str  := node_source(src, field.default_value)
					for name_expr in field.names {
						if p_ident, p_ok := name_expr.derived.(^ast.Ident); p_ok {
							append(&bspec.params, Behavior_Param{
								name         = p_ident.name,
								type_expr    = type_str,
								default_expr = def_str,
							})
						}
					}
				}

				append(pending_behaviors, bspec)
			}
		}
	}

	return true
}

// Bind all parsed `@(behavior)` specs to their matching `@(actor)` struct.
finalize_module_spec :: proc(mod: ^Module_Spec, pending_behaviors: []Behavior_Spec) {
	for b in pending_behaviors {
		for &a in mod.actors {
			if a.name == b.actor_type {
				append(&a.behaviors, b)
				break
			}
		}
	}
}

// Generate the Odin source code for a `Module_Spec`.
emit_module_code :: proc(mod: ^Module_Spec, allocator := context.allocator) -> string {
	sb := strings.builder_make(allocator)

	pkg := mod.package_name if len(mod.package_name) > 0 else "main"
	actor_imp := mod.actor_import if len(mod.actor_import) > 0 else "\"actor\""

	fmt.sbprintln(&sb, "// Code generated by Sleipnir AST Metaprogramming Generator (`tools/sleipnir_gen`). DO NOT EDIT.")
	fmt.sbprintf(&sb, "package %s\n\n", pkg)
	fmt.sbprintf(&sb, "import %s\n\n", actor_imp)

	for a in mod.actors {
		if len(a.behaviors) == 0 do continue
		snake_actor := to_snake_case(a.name)
		defer delete(snake_actor)

		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintf(&sb, "// Generated Actor Bindings for `%s`\n", a.name)
		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintln(&sb)

		// 1. Per-behavior message structs
		for b in a.behaviors {
			msg_struct_name := fmt.tprintf("%s_Msg_%s", a.name, b.proc_name)
			if len(b.params) == 0 {
				fmt.sbprintf(&sb, "%s :: struct {{}}\n\n", msg_struct_name)
			} else {
				fmt.sbprintf(&sb, "%s :: struct {{\n", msg_struct_name)
				for p in b.params {
					fmt.sbprintf(&sb, "\t%s: %s,\n", p.name, p.type_expr)
				}
				fmt.sbprintln(&sb, "}\n")
			}
		}

		// 2. Tagged union & Actor / PID aliases
		fmt.sbprintf(&sb, "%s_Msg :: union {{\n", a.name)
		for b in a.behaviors {
			fmt.sbprintf(&sb, "\t%s_Msg_%s,\n", a.name, b.proc_name)
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_Actor :: actor.Actor(%s, %s_Msg)\n", a.name, a.name, a.name)
		fmt.sbprintf(&sb, "%s_PID   :: actor.PID(%s_Msg)\n\n", a.name, a.name)

		// 3. Generated `dispatch` procedure
		fmt.sbprintf(&sb, "%s_dispatch :: proc(ctx: ^actor.Context, self: ^%s, msg: ^%s_Msg) {{\n", snake_actor, a.name, a.name)
		fmt.sbprintln(&sb, "\tswitch m in msg^ {")
		for b in a.behaviors {
			fmt.sbprintf(&sb, "\tcase %s_Msg_%s:\n", a.name, b.proc_name)
			fmt.sbprintf(&sb, "\t\t%s(", b.proc_name)
			if b.takes_ctx {
				fmt.sbprint(&sb, "ctx, self")
			} else {
				fmt.sbprint(&sb, "self")
			}
			for p in b.params {
				fmt.sbprintf(&sb, ", m.%s", p.name)
			}
			fmt.sbprintln(&sb, ")")
		}
		fmt.sbprintln(&sb, "\t}")
		fmt.sbprintln(&sb, "}\n")

		// 4. Generated `spawn_<actor>` constructors
		fmt.sbprintf(&sb, "spawn_%s_auto :: proc(\n", snake_actor)
		fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
		fmt.sbprintln(&sb, "\tid: u32 = 0,")
		fmt.sbprintln(&sb, "\tallocator := context.allocator,")
		fmt.sbprintf(&sb, ") -> ^%s_Actor {{\n", a.name)
		fmt.sbprintf(&sb, "\ta, _ := actor.spawn(initial_state, %s_dispatch, id, allocator)\n", snake_actor)
		fmt.sbprintln(&sb, "\treturn a")
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s_sched :: proc(\n", snake_actor)
		fmt.sbprintln(&sb, "\ts: ^actor.Scheduler,")
		fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
		fmt.sbprintln(&sb, "\tid: u32 = 0,")
		fmt.sbprintln(&sb, "\tallocator := context.allocator,")
		fmt.sbprintf(&sb, ") -> ^%s_Actor {{\n", a.name)
		fmt.sbprintf(&sb, "\ta, _ := actor.spawn(s, initial_state, %s_dispatch, id, allocator)\n", snake_actor)
		fmt.sbprintln(&sb, "\treturn a")
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s :: proc {{\n", snake_actor)
		fmt.sbprintf(&sb, "\tspawn_%s_auto,\n", snake_actor)
		fmt.sbprintf(&sb, "\tspawn_%s_sched,\n", snake_actor)
		fmt.sbprintln(&sb, "}\n")

		// 5. Generated `be_<behavior>` and `send_<behavior>` async callers
		for b in a.behaviors {
			msg_struct_name := fmt.tprintf("%s_Msg_%s", a.name, b.proc_name)

			// Build parameter signature suffix and struct literal initializer
			param_sig_b := strings.builder_make(context.temp_allocator)
			lit_init_b  := strings.builder_make(context.temp_allocator)
			for p, idx in b.params {
				if len(p.default_expr) > 0 {
					fmt.sbprintf(&param_sig_b, ", %s: %s = %s", p.name, p.type_expr, p.default_expr)
				} else {
					fmt.sbprintf(&param_sig_b, ", %s: %s", p.name, p.type_expr)
				}
				if idx > 0 {
					fmt.sbprint(&lit_init_b, ", ")
				}
				fmt.sbprintf(&lit_init_b, "%s = %s", p.name, p.name)
			}
			param_sig := strings.to_string(param_sig_b)
			lit_init  := strings.to_string(lit_init_b)

			fmt.sbprintf(&sb, "send_%s_actor :: #force_inline proc(target: ^%s_Actor%s) {{\n", b.proc_name, a.name, param_sig)
			fmt.sbprintf(&sb, "\tactor.send(target, %s_Msg(%s{{%s}}))\n", a.name, msg_struct_name, lit_init)
			fmt.sbprintln(&sb, "}\n")

			fmt.sbprintf(&sb, "send_%s_pid :: #force_inline proc(target: %s_PID%s) {{\n", b.proc_name, a.name, param_sig)
			fmt.sbprintf(&sb, "\tactor.send(target, %s_Msg(%s{{%s}}))\n", a.name, msg_struct_name, lit_init)
			fmt.sbprintln(&sb, "}\n")

			fmt.sbprintf(&sb, "send_%s :: proc {{\n", b.proc_name)
			fmt.sbprintf(&sb, "\tsend_%s_actor,\n", b.proc_name)
			fmt.sbprintf(&sb, "\tsend_%s_pid,\n", b.proc_name)
			fmt.sbprintln(&sb, "}\n")

			fmt.sbprintf(&sb, "be_%s :: send_%s\n\n", b.proc_name, b.proc_name)
		}
	}

	return strings.to_string(sb)
}

// High-level helper: parse a single Odin source string and return the generated Odin code.
generate_from_source :: proc(
	src: string,
	filename: string = "input.odin",
	allocator := context.allocator,
) -> (code: string, ok: bool) {
	mod: Module_Spec
	init_module_spec(&mod)
	defer destroy_module_spec(&mod)

	pending := make([dynamic]Behavior_Spec)
	defer delete(pending)

	if !parse_source_into_spec(src, filename, &mod, &pending) {
		return "", false
	}
	finalize_module_spec(&mod, pending[:])
	return emit_module_code(&mod, allocator), true
}

// Scan a directory of `.odin` files (skipping `*_gen.odin`), parse all `@(actor)`
// and `@(behavior)` declarations, and write `<dir>/actors_gen.odin`.
generate_directory :: proc(dir_path: string, out_filename: string = "actors_gen.odin") -> (out_path: string, ok: bool) {
	f, err := os.open(dir_path)
	if err != nil do return "", false
	defer os.close(f)

	entries, rerr := os.read_all_directory(f, context.allocator)
	if rerr != nil do return "", false
	defer os.file_info_slice_delete(entries, context.allocator)

	// Sort filenames deterministically
	filenames := make([dynamic]string)
	defer delete(filenames)
	for entry in entries {
		if entry.type == .Regular &&
		   strings.has_suffix(entry.name, ".odin") &&
		   !strings.has_suffix(entry.name, "_gen.odin") &&
		   entry.name != out_filename {
			append(&filenames, entry.name)
		}
	}
	slice.sort(filenames[:])

	mod: Module_Spec
	init_module_spec(&mod)
	defer destroy_module_spec(&mod)

	pending := make([dynamic]Behavior_Spec)
	defer delete(pending)

	file_buffers := make([dynamic][]byte)
	defer {
		for buf in file_buffers {
			delete(buf)
		}
		delete(file_buffers)
	}

	for name in filenames {
		full, _ := filepath.join({dir_path, name}, context.temp_allocator)
		data, read_err := os.read_entire_file(full, context.allocator)
		if read_err != nil do return "", false
		append(&file_buffers, data)
		if !parse_source_into_spec(string(data), full, &mod, &pending) {
			return "", false
		}
	}

	finalize_module_spec(&mod, pending[:])
	code := emit_module_code(&mod, context.allocator)
	defer delete(code)

	out_full, _ := filepath.join({dir_path, out_filename}, context.allocator)
	write_err := os.write_entire_file(out_full, transmute([]byte)code)
	if write_err != nil {
		delete(out_full)
		return "", false
	}
	return out_full, true
}
