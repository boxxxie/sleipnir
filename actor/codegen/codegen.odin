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
// Features:
//   1. Minimal Annotation (`@(actor)` only): Any procedure in the package taking
//      `self: ^Actor_Type` (or `ctx: ^actor.Context, self: ^Actor_Type`) is
//      automatically registered as a behavior of that `@(actor)` struct (unless
//      marked `@(private)` or `@(no_behavior)`). `@(behavior)` / `@(behavior="name")`
//      is also supported for explicit naming.
//   2. Request/Reply Return Values (`-> T`): Behaviors that return a value `-> T`
//      automatically generate synchronous `call_<name>(target) -> T` (`actor->name()`)
//      and async `send_<name>(target, ..., reply_to: actor.PID(T))` (where `target`
//      can be either `<Actor>_Actor` or `actor.PID(<Actor>_Msg)`) without needing
//      a `reply_to` field in the behavior's parameters.
//   3. Built-in Odin `->` Selector Call Syntax: Generated actor handles embed a
//      static `vtable` so `account->deposit(250)` and `bal := account->get_balance()`
//      work alongside `send_deposit(account, 250)` and `call_get_balance(account)`.
//   4. Multi-File Package Support: Scans all `.odin` files in a package directory,
//      forwards cross-file `import` declarations, and merges shared behavior names
//      into unified Odin procedure overload groups.
// =============================================================================

Behavior_Param :: struct {
	name:         string,
	type_expr:    string,
	default_expr: string,
}

Behavior_Spec :: struct {
	proc_name:   string, // Actual Odin procedure name (e.g. `deposit` or `account_reset`)
	short_name:  string, // Exposed method/behavior name (e.g. `deposit` or `reset`)
	actor_type:  string,
	takes_ctx:   bool,
	return_type: string, // Empty string for 1-way `send_` behaviors; non-empty for `call_` behaviors
	params:      [dynamic]Behavior_Param,
}

Actor_Spec :: struct {
	name:      string,
	behaviors: [dynamic]Behavior_Spec,
}

Import_Spec :: struct {
	alias:    string, // Effective local identifier (e.g. `time` for `import "core:time"`)
	raw_decl: string, // Full `import ...` declaration text
	fullpath: string,
}

Module_Spec :: struct {
	package_name: string,
	actor_import: string,
	imports:      [dynamic]Import_Spec,
	actors:       [dynamic]Actor_Spec,
	ast_arena:    mem.Dynamic_Arena,
}

init_module_spec :: proc(m: ^Module_Spec) {
	m.imports = make([dynamic]Import_Spec)
	m.actors  = make([dynamic]Actor_Spec)
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
	delete(m.imports)
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

// Check if a `Value_Decl` has `@(attr_name)` or `@(attr_name="value")`.
get_attribute :: proc(decl: ^ast.Value_Decl, attr_name: string) -> (val: string, found: bool) {
	for attr in decl.attributes {
		for elem in attr.elems {
			#partial switch e in elem.derived {
			case ^ast.Ident:
				if e.name == attr_name do return "", true
			case ^ast.Field_Value:
				if ident, ok := e.field.derived.(^ast.Ident); ok && ident.name == attr_name {
					if lit, lit_ok := e.value.derived.(^ast.Basic_Lit); lit_ok {
						return strings.trim(lit.tok.text, "\""), true
					}
					if v_ident, v_ok := e.value.derived.(^ast.Ident); v_ok {
						return v_ident.name, true
					}
					return "", true
				}
			}
		}
	}
	return "", false
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

// Derive the local package alias for an `Import_Decl` (e.g. `import "core:time"` -> `"time"`).
import_local_alias :: proc(imp: ^ast.Import_Decl) -> string {
	if imp.name.text != "" {
		return imp.name.text
	}
	clean := strings.trim(imp.fullpath, "\"")
	if colon := strings.last_index_byte(clean, ':'); colon >= 0 {
		clean = clean[colon + 1:]
	}
	if slash := strings.last_index_byte(clean, '/'); slash >= 0 {
		clean = clean[slash + 1:]
	}
	return clean
}

// Parse a single Odin source buffer and append discovered imports, `@(actor)` structs,
// and candidate behavior procedures into `mod` and `pending_behaviors`.
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
			alias := import_local_alias(d)
			if alias == "actor" || strings.has_suffix(d.fullpath, "actor\"") || strings.has_suffix(d.fullpath, "/actor\"") {
				mod.actor_import = d.fullpath
			} else {
				already := false
				for existing in mod.imports {
					if existing.alias == alias {
						already = true
						break
					}
				}
				if !already {
					append(&mod.imports, Import_Spec{
						alias    = alias,
						raw_decl = node_source(src, d),
						fullpath = d.fullpath,
					})
				}
			}
		case ^ast.Value_Decl:
			if d.is_mutable || len(d.names) == 0 || len(d.values) == 0 {
				continue
			}
			ident, ok := d.names[0].derived.(^ast.Ident)
			if !ok do continue

			if _, is_actor := get_attribute(d, "actor"); is_actor {
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
				continue
			}

			// Any top-level procedure taking `self: ^Actor_Type` (unless marked `@(private)`
			// or `@(no_behavior)`) is a candidate behavior if `Actor_Type` is an `@(actor)` struct!
			_, is_private     := get_attribute(d, "private")
			_, is_no_behavior := get_attribute(d, "no_behavior")
			custom_be_name, has_explicit_be := get_attribute(d, "behavior")
			if (is_private || is_no_behavior) && !has_explicit_be {
				continue
			}

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

			// Check for optional return type (`-> Return_Type`)
			ret_type := ""
			if proc_lit.type.results != nil && len(proc_lit.type.results.list) > 0 {
				res_field := proc_lit.type.results.list[0]
				if res_field.type != nil {
					ret_type = node_source(src, res_field.type)
				} else if len(res_field.names) > 0 {
					ret_type = node_source(src, res_field.names[0])
				}
			}

			short := custom_be_name if len(custom_be_name) > 0 else ident.name
			bspec := Behavior_Spec{
				proc_name   = ident.name,
				short_name  = short,
				actor_type  = actor_type,
				takes_ctx   = takes_ctx,
				return_type = ret_type,
				params      = make([dynamic]Behavior_Param),
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

	return true
}

// Bind all candidate behavior procedures to their matching `@(actor)` struct,
// and free parameter lists of non-actor procedures.
finalize_module_spec :: proc(mod: ^Module_Spec, pending_behaviors: []Behavior_Spec) {
	for b in pending_behaviors {
		matched := false
		for &a in mod.actors {
			if a.name == b.actor_type {
				append(&a.behaviors, b)
				matched = true
				break
			}
		}
		if !matched {
			delete(b.params)
		}
	}
}

// Check if an import alias (e.g. `time.`) is referenced in any behavior parameter, default, or return type.
is_import_referenced :: proc(mod: ^Module_Spec, alias: string) -> bool {
	prefix := fmt.tprintf("%s.", alias)
	for a in mod.actors {
		for b in a.behaviors {
			if strings.contains(b.return_type, prefix) {
				return true
			}
			for p in b.params {
				if strings.contains(p.type_expr, prefix) || strings.contains(p.default_expr, prefix) {
					return true
				}
			}
		}
	}
	return false
}

// Generate the Odin source code for a `Module_Spec`.
emit_module_code :: proc(mod: ^Module_Spec, allocator := context.allocator) -> string {
	sb := strings.builder_make(allocator)

	pkg := mod.package_name if len(mod.package_name) > 0 else "main"
	actor_imp := mod.actor_import if len(mod.actor_import) > 0 else "\"actor\""

	fmt.sbprintln(&sb, "// Code generated by Sleipnir AST Metaprogramming Generator (`tools/sleipnir_gen`). DO NOT EDIT.")
	fmt.sbprintf(&sb, "package %s\n\n", pkg)
	fmt.sbprintf(&sb, "import %s\n", actor_imp)
	for imp in mod.imports {
		if is_import_referenced(mod, imp.alias) {
			fmt.sbprintf(&sb, "%s\n", imp.raw_decl)
		}
	}
	fmt.sbprintln(&sb)

	Group_Entry :: struct {
		name:  string,
		procs: [dynamic]string,
	}
	send_groups := make([dynamic]Group_Entry, context.temp_allocator)
	call_groups := make([dynamic]Group_Entry, context.temp_allocator)

	add_to_group :: proc(groups: ^[dynamic]Group_Entry, group_name: string, proc_sym: string) {
		for &g in groups^ {
			if g.name == group_name {
				append(&g.procs, proc_sym)
				return
			}
		}
		procs := make([dynamic]string, context.temp_allocator)
		append(&procs, proc_sym)
		append(groups, Group_Entry{name = group_name, procs = procs})
	}

	add_pair_to_group :: proc(groups: ^[dynamic]Group_Entry, b: Behavior_Spec, fn_a, fn_b: string) {
		add_to_group(groups, b.short_name, fn_a)
		add_to_group(groups, b.short_name, fn_b)
		if b.proc_name != b.short_name {
			add_to_group(groups, b.proc_name, fn_a)
			add_to_group(groups, b.proc_name, fn_b)
		}
	}

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
			msg_struct_name := fmt.tprintf("%s_Msg_%s", a.name, b.short_name)
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

		// 2. Tagged union, VTable (for `->` selector calls), and Actor / PID handles
		fmt.sbprintf(&sb, "%s_Msg :: union {{\n", a.name)
		for b in a.behaviors {
			fmt.sbprintf(&sb, "\t%s_Msg_%s,\n", a.name, b.short_name)
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_VTable :: struct {{\n", a.name)
		for b in a.behaviors {
			param_sig_b := strings.builder_make(context.temp_allocator)
			for p in b.params {
				if len(p.default_expr) > 0 {
					fmt.sbprintf(&param_sig_b, ", %s: %s = %s", p.name, p.type_expr, p.default_expr)
				} else {
					fmt.sbprintf(&param_sig_b, ", %s: %s", p.name, p.type_expr)
				}
			}
			param_sig := strings.to_string(param_sig_b)
			if len(b.return_type) > 0 {
				fmt.sbprintf(&sb, "\t%s: proc(target: %s_Actor%s) -> %s,\n", b.short_name, a.name, param_sig, b.return_type)
			} else {
				fmt.sbprintf(&sb, "\t%s: proc(target: %s_Actor%s),\n", b.short_name, a.name, param_sig)
			}
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_Actor :: struct {{\n", a.name)
		fmt.sbprintf(&sb, "\tusing raw:    ^actor.Actor(%s, %s_Msg),\n", a.name, a.name)
		fmt.sbprintf(&sb, "\tusing vtable: ^%s_VTable,\n", a.name)
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_PID :: %s_Actor\n\n", a.name, a.name)

		// 3. Generated `dispatch` procedure
		fmt.sbprintf(&sb, "%s_dispatch :: proc(ctx: ^actor.Context, self: ^%s, msg: ^%s_Msg) {{\n", snake_actor, a.name, a.name)
		fmt.sbprintln(&sb, "\tswitch m in msg^ {")
		for b in a.behaviors {
			fmt.sbprintf(&sb, "\tcase %s_Msg_%s:\n", a.name, b.short_name)
			if len(b.return_type) > 0 {
				fmt.sbprintf(&sb, "\t\tres := %s(", b.proc_name)
			} else {
				fmt.sbprintf(&sb, "\t\t%s(", b.proc_name)
			}
			if b.takes_ctx {
				fmt.sbprint(&sb, "ctx, self")
			} else {
				fmt.sbprint(&sb, "self")
			}
			for p in b.params {
				fmt.sbprintf(&sb, ", m.%s", p.name)
			}
			fmt.sbprintln(&sb, ")")
			if len(b.return_type) > 0 {
				fmt.sbprintln(&sb, "\t\tactor.reply(ctx, res)")
			}
		}
		fmt.sbprintln(&sb, "\t}")
		fmt.sbprintln(&sb, "}\n")

		// 4. Generated `send_<actor>_<behavior>` and `call_<actor>_<behavior>` procedures
		for b in a.behaviors {
			msg_struct_name := fmt.tprintf("%s_Msg_%s", a.name, b.short_name)

			param_sig_b    := strings.builder_make(context.temp_allocator)
			no_def_sig_b   := strings.builder_make(context.temp_allocator)
			lit_init_b     := strings.builder_make(context.temp_allocator)
			for p, idx in b.params {
				if len(p.default_expr) > 0 {
					fmt.sbprintf(&param_sig_b, ", %s: %s = %s", p.name, p.type_expr, p.default_expr)
				} else {
					fmt.sbprintf(&param_sig_b, ", %s: %s", p.name, p.type_expr)
				}
				fmt.sbprintf(&no_def_sig_b, ", %s: %s", p.name, p.type_expr)
				if idx > 0 {
					fmt.sbprint(&lit_init_b, ", ")
				}
				fmt.sbprintf(&lit_init_b, "%s = %s", p.name, p.name)
			}
			param_sig  := strings.to_string(param_sig_b)
			no_def_sig := strings.to_string(no_def_sig_b)
			lit_init   := strings.to_string(lit_init_b)

			if len(b.return_type) > 0 {
				// Behavior returns `-> Return_Type`:
				// 1) `call_<actor>_<behavior>_actor` & `_raw` (synchronous request/reply)
				call_actor_fn := fmt.tprintf("call_%s_%s_actor", snake_actor, b.short_name)
				call_pid_fn   := fmt.tprintf("call_%s_%s_pid", snake_actor, b.short_name)

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: %s_Actor%s) -> %s {{\n", call_actor_fn, a.name, param_sig, b.return_type)
				fmt.sbprintf(&sb, "\treturn actor.call(target.raw, %s_Msg(%s{{%s}}), %s)\n", a.name, msg_struct_name, lit_init, b.return_type)
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: actor.PID(%s_Msg)%s) -> %s {{\n", call_pid_fn, a.name, param_sig, b.return_type)
				fmt.sbprintf(&sb, "\treturn actor.call(target, %s_Msg(%s{{%s}}), %s)\n", a.name, msg_struct_name, lit_init, b.return_type)
				fmt.sbprintln(&sb, "}\n")

				add_pair_to_group(&call_groups, b, call_actor_fn, call_pid_fn)

				// 2) `send_<actor>_<behavior>_reply_actor` & `_reply_pid` (async forward of return value to a `reply_to` PID)
				send_reply_actor_fn := fmt.tprintf("send_%s_%s_reply_actor", snake_actor, b.short_name)
				send_reply_pid_fn   := fmt.tprintf("send_%s_%s_reply_pid", snake_actor, b.short_name)

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: %s_Actor%s, reply_to: actor.PID(%s)) {{\n", send_reply_actor_fn, a.name, no_def_sig, b.return_type)
				fmt.sbprintf(&sb, "\tactor.send(target.raw, %s_Msg(%s{{%s}}), reply_to)\n", a.name, msg_struct_name, lit_init)
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: actor.PID(%s_Msg)%s, reply_to: actor.PID(%s)) {{\n", send_reply_pid_fn, a.name, no_def_sig, b.return_type)
				fmt.sbprintf(&sb, "\tactor.send(target, %s_Msg(%s{{%s}}), reply_to)\n", a.name, msg_struct_name, lit_init)
				fmt.sbprintln(&sb, "}\n")

				add_pair_to_group(&send_groups, b, send_reply_actor_fn, send_reply_pid_fn)
			} else {
				// 1-way fire-and-forget behavior (`send_<name>`)
				actor_fn := fmt.tprintf("send_%s_%s_actor", snake_actor, b.short_name)
				pid_fn   := fmt.tprintf("send_%s_%s_pid", snake_actor, b.short_name)

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: %s_Actor%s) {{\n", actor_fn, a.name, param_sig)
				fmt.sbprintf(&sb, "\tactor.send(target.raw, %s_Msg(%s{{%s}}))\n", a.name, msg_struct_name, lit_init)
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(target: actor.PID(%s_Msg)%s) {{\n", pid_fn, a.name, param_sig)
				fmt.sbprintf(&sb, "\tactor.send(target, %s_Msg(%s{{%s}}))\n", a.name, msg_struct_name, lit_init)
				fmt.sbprintln(&sb, "}\n")

				add_pair_to_group(&send_groups, b, actor_fn, pid_fn)
			}
		}

		// 5. Static VTable instance & `spawn_<actor>` constructors
		fmt.sbprintf(&sb, "@(private)\n%s_vtable := %s_VTable{{\n", snake_actor, a.name)
		for b in a.behaviors {
			if len(b.return_type) > 0 {
				fmt.sbprintf(&sb, "\t%s = call_%s_%s_actor,\n", b.short_name, snake_actor, b.short_name)
			} else {
				fmt.sbprintf(&sb, "\t%s = send_%s_%s_actor,\n", b.short_name, snake_actor, b.short_name)
			}
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s_auto :: proc(\n", snake_actor)
		fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
		fmt.sbprintln(&sb, "\tid: u32 = 0,")
		fmt.sbprintln(&sb, "\tallocator := context.allocator,")
		fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
		fmt.sbprintf(&sb, "\ta, _ := actor.spawn(initial_state, %s_dispatch, id, allocator)\n", snake_actor)
		fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_actor)
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s_sched :: proc(\n", snake_actor)
		fmt.sbprintln(&sb, "\ts: ^actor.Scheduler,")
		fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
		fmt.sbprintln(&sb, "\tid: u32 = 0,")
		fmt.sbprintln(&sb, "\tallocator := context.allocator,")
		fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
		fmt.sbprintf(&sb, "\ta, _ := actor.spawn(s, initial_state, %s_dispatch, id, allocator)\n", snake_actor)
		fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_actor)
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s :: proc {{\n", snake_actor)
		fmt.sbprintf(&sb, "\tspawn_%s_auto,\n", snake_actor)
		fmt.sbprintf(&sb, "\tspawn_%s_sched,\n", snake_actor)
		fmt.sbprintln(&sb, "}\n")
	}

	// 6. Emit unified `send_<behavior>` and `call_<behavior>` procedure overload groups
	if len(send_groups) > 0 || len(call_groups) > 0 {
		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintln(&sb, "// Unified Behavior Overload Groups (`send_<behavior>` & `call_<behavior>`)")
		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintln(&sb)

		for g in send_groups {
			fmt.sbprintf(&sb, "send_%s :: proc {{\n", g.name)
			for fn_name in g.procs {
				fmt.sbprintf(&sb, "\t%s,\n", fn_name)
			}
			fmt.sbprintln(&sb, "}\n")
		}

		for g in call_groups {
			fmt.sbprintf(&sb, "call_%s :: proc {{\n", g.name)
			for fn_name in g.procs {
				fmt.sbprintf(&sb, "\t%s,\n", fn_name)
			}
			fmt.sbprintln(&sb, "}\n")
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
	return generate_from_sources({src}, {filename}, allocator)
}

// High-level helper: parse multiple Odin source strings belonging to the same package
// and return the combined generated Odin code.
generate_from_sources :: proc(
	sources: []string,
	filenames: []string = nil,
	allocator := context.allocator,
) -> (code: string, ok: bool) {
	mod: Module_Spec
	init_module_spec(&mod)
	defer destroy_module_spec(&mod)

	pending := make([dynamic]Behavior_Spec)
	defer delete(pending)

	for src, idx in sources {
		fname := filenames[idx] if idx < len(filenames) else "input.odin"
		if !parse_source_into_spec(src, fname, &mod, &pending) {
			for b in pending do delete(b.params)
			return "", false
		}
	}
	finalize_module_spec(&mod, pending[:])
	return emit_module_code(&mod, allocator), true
}

// Scan a directory of `.odin` files (skipping `*_gen.odin`), parse all `@(actor)`
// and behavior declarations across all files in the package, and write `<dir>/actors_gen.odin`.
generate_directory :: proc(dir_path: string, out_filename: string = "actors_gen.odin") -> (out_path: string, ok: bool) {
	f, err := os.open(dir_path)
	if err != nil do return "", false
	defer os.close(f)

	entries, rerr := os.read_all_directory(f, context.allocator)
	if rerr != nil do return "", false
	defer os.file_info_slice_delete(entries, context.allocator)

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
		if read_err != nil {
			for b in pending do delete(b.params)
			return "", false
		}
		append(&file_buffers, data)
		if !parse_source_into_spec(string(data), full, &mod, &pending) {
			for b in pending do delete(b.params)
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
