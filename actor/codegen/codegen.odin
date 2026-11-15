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
	is_variadic:  bool,
}

Result_Field :: struct {
	name:      string,
	type_expr: string,
}

Behavior_Spec :: struct {
	proc_name:     string, // Actual Odin procedure name (e.g. `deposit` or `account_reset`)
	short_name:    string, // Exposed method/behavior name (e.g. `deposit` or `reset`)
	actor_type:    string,
	takes_ctx:     bool,
	return_type:   string, // Empty string for 1-way `send_` behaviors; non-empty for `call_` behaviors
	result_struct: string, // Non-empty for multi-return behaviors (e.g. `Actor_proc_Result`)
	result_fields: [dynamic]Result_Field,
	params:        [dynamic]Behavior_Param,
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
	package_name:  string,
	actor_import:  string,
	actor_alias:   string,
	actor_raw_imp: string,
	imports:       [dynamic]Import_Spec,
	actors:        [dynamic]Actor_Spec,
	ast_arena:     mem.Dynamic_Arena,
}

Diagnostic :: struct {
	file:    string,
	line:    int,
	message: string,
}

init_module_spec :: proc(m: ^Module_Spec) {
	m.actor_alias = "actor"
	m.imports     = make([dynamic]Import_Spec)
	m.actors      = make([dynamic]Actor_Spec)
	mem.dynamic_arena_init(&m.ast_arena)
}

destroy_module_spec :: proc(m: ^Module_Spec) {
	for &a in m.actors {
		for &b in a.behaviors {
			delete(b.params)
			delete(b.result_fields)
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

// Check if a `Value_Decl` has `@(attr_name)` or `@(attr_name="value")` / `@(attr_name=`value`)`.
get_attribute :: proc(decl: ^ast.Value_Decl, attr_name: string) -> (val: string, found: bool) {
	for attr in decl.attributes {
		for elem in attr.elems {
			#partial switch e in elem.derived {
			case ^ast.Ident:
				if e.name == attr_name {
					return "", true
				}
			case ^ast.Field_Value:
				if ident, ok := e.field.derived.(^ast.Ident); ok && ident.name == attr_name {
					if lit, lit_ok := e.value.derived.(^ast.Basic_Lit); lit_ok {
						return strings.trim(lit.tok.text, "\"`"), true
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
	if node == nil {
		return ""
	}
	start := node.pos.offset
	end   := node.end.offset
	if start >= 0 && end >= start && end <= len(src) {
		return strings.trim_space(src[start:end])
	}
	return ""
}

// Extract the pointed-to struct name and optional package qualifier if `expr` is `^Actor_Name` or `^pkg.Context`.
extract_pointer_ident :: proc(expr: ^ast.Expr) -> (name: string, pkg: string, ok: bool) {
	if expr == nil {
		return "", "", false
	}
	if ptr, ptr_ok := expr.derived.(^ast.Pointer_Type); ptr_ok {
		if ident, ident_ok := ptr.elem.derived.(^ast.Ident); ident_ok {
			return ident.name, "", true
		}
		if sel, sel_ok := ptr.elem.derived.(^ast.Selector_Expr); sel_ok {
			pkg_name := ""
			if x_ident, x_ok := sel.expr.derived.(^ast.Ident); x_ok {
				pkg_name = x_ident.name
			}
			if field_ident, f_ok := sel.field.derived.(^ast.Ident); f_ok && len(pkg_name) > 0 {
				return field_ident.name, pkg_name, true
			}
		}
	}
	return "", "", false
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

is_actor_import_path :: proc(fullpath: string, alias: string) -> bool {
	clean := strings.trim(fullpath, "\"")
	if clean == "actor" || strings.has_suffix(clean, "/actor") || strings.has_suffix(clean, ":actor") {
		return true
	}
	return alias == "actor" && strings.contains(clean, "actor")
}

// Detect file-level `#+private file` tag before `package`.
has_file_private_tag :: proc(src: string) -> bool {
	rem := src
	for line in strings.split_lines_iterator(&rem) {
		trimmed := strings.trim_space(line)
		if strings.has_prefix(trimmed, "package ") || strings.has_prefix(trimmed, "package\t") {
			break
		}
		if strings.has_prefix(trimmed, "#+private") {
			rest := strings.trim_space(trimmed[len("#+private"):])
			if rest == "file" {
				return true
			}
		}
	}
	return false
}

current_os_tag :: proc() -> string {
	#partial switch ODIN_OS {
	case .Linux:   return "linux"
	case .Windows: return "windows"
	case .Darwin:  return "darwin"
	case .FreeBSD: return "freebsd"
	case .OpenBSD: return "openbsd"
	case .NetBSD:  return "netbsd"
	case .JS:      return "js"
	case .WASI:    return "wasi"
	case .Orca:    return "orca"
	}
	return ""
}

// Check OS filename suffixes (e.g. `*_windows.odin`, `*_darwin.odin`, `*_linux.odin`).
matches_filename_os :: proc(name: string) -> bool {
	base := strings.trim_suffix(name, ".odin")
	cur := current_os_tag()
	for entry in ([]string{"_windows", "_linux", "_darwin", "_freebsd", "_openbsd", "_netbsd", "_haiku", "_js", "_wasi", "_orca"}) {
		if strings.has_suffix(base, entry) {
			return entry[1:] == cur
		}
	}
	return true
}

matches_single_build_term :: proc(term: string, cur_os: string) -> bool {
	t := strings.trim_space(term)
	if len(t) == 0 {
		return true
	}
	negated := false
	if t[0] == '!' {
		negated = true
		t = strings.trim_space(t[1:])
	}
	known_oses := []string{"windows", "linux", "darwin", "freebsd", "openbsd", "netbsd", "haiku", "js", "wasi", "orca"}
	is_known_os := false
	for k in known_oses {
		if t == k {
			is_known_os = true
			break
		}
	}
	if !is_known_os {
		// Unknown/custom feature tags default to matched unless contradicted by OS tags.
		return true
	}
	matched := t == cur_os
	return !matched if negated else matched
}

// Check `#+build ...` directives before `package`.
matches_build_tags :: proc(src: string) -> bool {
	cur_os := current_os_tag()
	rem := src
	for line in strings.split_lines_iterator(&rem) {
		trimmed := strings.trim_space(line)
		if strings.has_prefix(trimmed, "package ") || strings.has_prefix(trimmed, "package\t") {
			break
		}
		if strings.has_prefix(trimmed, "#+build") {
			rest := strings.trim_space(trimmed[len("#+build"):])
			if len(rest) == 0 {
				continue
			}
			// Within a `#+build` line, commas OR spaces act as OR alternatives unless `!` negates.
			any_match := false
			tag_rem := rest
			for part in strings.split_by_byte_iterator(&tag_rem, ',') {
				sub := strings.trim_space(part)
				if len(sub) == 0 {
					continue
				}
				space_rem := sub
				all_sub := true
				for word in strings.split_by_byte_iterator(&space_rem, ' ') {
					if len(word) == 0 {
						continue
					}
					if !matches_single_build_term(word, cur_os) {
						all_sub = false
					}
				}
				if all_sub {
					any_match = true
					break
				}
			}
			if !any_match {
				return false
			}
		}
	}
	return true
}

infer_type_from_default :: proc(expr: ^ast.Expr, src: string) -> (string, bool) {
	if expr == nil {
		return "", false
	}
	#partial switch e in expr.derived {
	case ^ast.Paren_Expr:
		return infer_type_from_default(e.expr, src)
	case ^ast.Unary_Expr:
		return infer_type_from_default(e.expr, src)
	case ^ast.Basic_Lit:
		#partial switch e.tok.kind {
		case .Integer: return "int", true
		case .Float:   return "f64", true
		case .Imag:    return "complex128", true
		case .Rune:    return "rune", true
		case .String:  return "string", true
		}
	case ^ast.Ident:
		if e.name == "true" || e.name == "false" {
			return "bool", true
		}
	case ^ast.Comp_Lit:
		if e.type != nil {
			t_str := node_source(src, e.type)
			if len(t_str) > 0 {
				return t_str, true
			}
		}
	case ^ast.Type_Cast:
		if e.type != nil {
			t_str := node_source(src, e.type)
			if len(t_str) > 0 {
				return t_str, true
			}
		}
	}
	return "", false
}

expr_has_poly :: proc(expr: ^ast.Expr) -> bool {
	if expr == nil {
		return false
	}
	#partial switch e in expr.derived {
	case ^ast.Poly_Type:
		return true
	case ^ast.Pointer_Type:
		return expr_has_poly(e.elem)
	case ^ast.Array_Type:
		return expr_has_poly(e.elem) || expr_has_poly(e.len)
	case ^ast.Dynamic_Array_Type:
		return expr_has_poly(e.elem)
	case ^ast.Map_Type:
		return expr_has_poly(e.key) || expr_has_poly(e.value)
	case ^ast.Ellipsis:
		return expr_has_poly(e.expr)
	case ^ast.Call_Expr:
		if expr_has_poly(e.expr) {
			return true
		}
		for arg in e.args {
			if expr_has_poly(arg) {
				return true
			}
		}
	}
	return false
}

has_poly_params :: proc(fields: []^ast.Field) -> bool {
	for f in fields {
		if expr_has_poly(f.type) {
			return true
		}
	}
	return false
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

	if has_file_private_tag(src) {
		return true
	}

	file_actor_alias := "actor"

	for decl_stmt in file.decls {
		#partial switch d in decl_stmt.derived {
		case ^ast.Import_Decl:
			alias := import_local_alias(d)
			if is_actor_import_path(d.fullpath, alias) {
				file_actor_alias = alias
				if mod.actor_raw_imp == "" {
					mod.actor_import  = d.fullpath
					mod.actor_alias   = alias
					mod.actor_raw_imp = node_source(src, d)
				}
			} else {
				already := false
				for existing in mod.imports {
					if existing.alias == alias && existing.fullpath == d.fullpath {
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
			if !ok {
				continue
			}

			if _, is_actor := get_attribute(d, "actor"); is_actor {
				if st, is_struct := d.values[0].derived.(^ast.Struct_Type); is_struct {
					if st.poly_params != nil {
						continue
					}
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
			if len(fields) == 0 {
				continue
			}
			if proc_lit.type.generic || len(proc_lit.where_clauses) > 0 || has_poly_params(fields) {
				continue
			}

			// Determine whether param 0 is `self: ^Actor` or `(ctx: ^Context, self: ^Actor)`
			actor_type := ""
			takes_ctx  := false
			start_field_idx := 1

			if first_name, first_pkg, ok0 := extract_pointer_ident(fields[0].type); ok0 {
				if first_name == "Context" && (first_pkg == "" || first_pkg == file_actor_alias) && len(fields) >= 2 {
					if second_name, second_pkg, ok1 := extract_pointer_ident(fields[1].type); ok1 && second_pkg == "" {
						actor_type = second_name
						takes_ctx  = true
						start_field_idx = 2
					}
				} else if first_pkg == "" {
					actor_type = first_name
					takes_ctx  = false
					start_field_idx = 1
				}
			}

			if actor_type == "" {
				continue
			}

			short := custom_be_name if len(custom_be_name) > 0 else ident.name
			bspec := Behavior_Spec{
				proc_name     = ident.name,
				short_name    = short,
				actor_type    = actor_type,
				takes_ctx     = takes_ctx,
				result_fields = make([dynamic]Result_Field),
				params        = make([dynamic]Behavior_Param),
			}

			// Check for optional return type (`-> Return_Type` or `-> (A, B)`)
			if proc_lit.type.results != nil && len(proc_lit.type.results.list) > 0 {
				res_list := proc_lit.type.results.list
				total_res := 0
				for rf in res_list {
					if rf.type != nil && len(rf.names) > 0 {
						total_res += len(rf.names)
					} else {
						total_res += 1
					}
				}
				if total_res == 1 {
					res_field := res_list[0]
					if res_field.type != nil {
						bspec.return_type = node_source(src, res_field.type)
					} else if len(res_field.names) > 0 {
						bspec.return_type = node_source(src, res_field.names[0])
					}
				} else if total_res > 1 {
					bspec.result_struct = fmt.aprintf("%s_%s_Result", actor_type, short, allocator = arena_alloc)
					sig_b := strings.builder_make(arena_alloc)
					strings.write_byte(&sig_b, '(')
					r_idx := 0
					for rf in res_list {
						if rf.type != nil {
							r_type := node_source(src, rf.type)
							count := len(rf.names) if len(rf.names) > 0 else 1
							for _ in 0 ..< count {
								if r_idx > 0 {
									strings.write_string(&sig_b, ", ")
								}
								strings.write_string(&sig_b, r_type)
								append(&bspec.result_fields, Result_Field{
									name      = fmt.aprintf("r%d", r_idx, allocator = arena_alloc),
									type_expr = r_type,
								})
								r_idx += 1
							}
						} else {
							for rn in rf.names {
								if r_idx > 0 {
									strings.write_string(&sig_b, ", ")
								}
								r_type := node_source(src, rn)
								strings.write_string(&sig_b, r_type)
								append(&bspec.result_fields, Result_Field{
									name      = fmt.aprintf("r%d", r_idx, allocator = arena_alloc),
									type_expr = r_type,
								})
								r_idx += 1
							}
						}
					}
					strings.write_byte(&sig_b, ')')
					bspec.return_type = strings.to_string(sig_b)
				}
			}

			param_valid := true
			anon_idx := 0
			for f_idx := start_field_idx; f_idx < len(fields); f_idx += 1 {
				field := fields[f_idx]
				type_str := node_source(src, field.type)
				def_str  := node_source(src, field.default_value)
				is_variadic := false
				if field.type != nil {
					if el, el_ok := field.type.derived.(^ast.Ellipsis); el_ok && el.expr != nil {
						type_str = fmt.aprintf("[]%s", node_source(src, el.expr), allocator = arena_alloc)
						is_variadic = true
					}
				} else if field.default_value != nil {
					if inferred, inf_ok := infer_type_from_default(field.default_value, src); inf_ok {
						type_str = inferred
					}
				}
				if len(type_str) == 0 {
					param_valid = false
					break
				}
				for name_expr in field.names {
					if p_ident, p_ok := name_expr.derived.(^ast.Ident); p_ok {
						pname := p_ident.name
						if pname == "_" {
							pname = fmt.aprintf("_arg%d", anon_idx, allocator = arena_alloc)
							anon_idx += 1
						}
						append(&bspec.params, Behavior_Param{
							name         = pname,
							type_expr    = type_str,
							default_expr = def_str,
							is_variadic  = is_variadic,
						})
					}
				}
			}

			if !param_valid {
				delete(bspec.params)
				delete(bspec.result_fields)
				continue
			}

			append(pending_behaviors, bspec)
		}
	}

	return true
}

// Bind all candidate behavior procedures to their matching `@(actor)` struct,
// deduplicating behavior names and freeing parameter lists of non-actor procedures.
finalize_module_spec :: proc(mod: ^Module_Spec, pending_behaviors: []Behavior_Spec) {
	for b in pending_behaviors {
		matched := false
		for &a in mod.actors {
			if a.name == b.actor_type {
				dup := false
				for existing in a.behaviors {
					if existing.short_name == b.short_name {
						dup = true
						break
					}
				}
				if dup {
					delete(b.params)
					delete(b.result_fields)
				} else {
					append(&a.behaviors, b)
				}
				matched = true
				break
			}
		}
		if !matched {
			delete(b.params)
			delete(b.result_fields)
		}
	}
}

is_ident_char :: proc(c: byte) -> bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// Token-boundary check for `alias.` in an expression string, ignoring string literals and identifier suffixes.
has_pkg_reference :: proc(expr: string, alias: string) -> bool {
	if len(expr) == 0 || len(alias) == 0 {
		return false
	}
	in_str: byte = 0
	escaped := false
	n := len(expr)
	alen := len(alias)
	for i := 0; i < n; i += 1 {
		ch := expr[i]
		if in_str != 0 {
			if in_str == '"' || in_str == '\'' {
				if escaped {
					escaped = false
					continue
				}
				if ch == '\\' {
					escaped = true
					continue
				}
			}
			if ch == in_str {
				in_str = 0
			}
			continue
		}
		if ch == '"' || ch == '`' || ch == '\'' {
			in_str = ch
			continue
		}
		if i + alen < n && expr[i:i + alen] == alias && expr[i + alen] == '.' {
			if i == 0 || !is_ident_char(expr[i - 1]) {
				return true
			}
		}
	}
	return false
}

// Check if an import alias (e.g. `time.`) is referenced in any behavior parameter, default, or return type.
is_import_referenced :: proc(mod: ^Module_Spec, alias: string) -> bool {
	for a in mod.actors {
		for b in a.behaviors {
			if has_pkg_reference(b.return_type, alias) {
				return true
			}
			for rf in b.result_fields {
				if has_pkg_reference(rf.type_expr, alias) {
					return true
				}
			}
			for p in b.params {
				if has_pkg_reference(p.type_expr, alias) || has_pkg_reference(p.default_expr, alias) {
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

	pkg     := mod.package_name if len(mod.package_name) > 0 else "main"
	a_alias := mod.actor_alias if len(mod.actor_alias) > 0 else "actor"

	has_emitted := false
	for a in mod.actors {
		if len(a.behaviors) > 0 {
			has_emitted = true
			break
		}
	}

	fmt.sbprintln(&sb, "// Code generated by Sleipnir AST Metaprogramming Generator (`tools/sleipnir_gen`). DO NOT EDIT.")
	fmt.sbprintf(&sb, "package %s\n\n", pkg)
	if has_emitted {
		if len(mod.actor_raw_imp) > 0 {
			fmt.sbprintf(&sb, "%s\n", mod.actor_raw_imp)
		} else if len(mod.actor_import) > 0 {
			fmt.sbprintf(&sb, "import %s\n", mod.actor_import)
		} else {
			fmt.sbprintln(&sb, "import \"actor\"")
		}
		for imp in mod.imports {
			if is_import_referenced(mod, imp.alias) {
				fmt.sbprintf(&sb, "%s\n", imp.raw_decl)
			}
		}
		fmt.sbprintln(&sb)
	}

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
		if len(a.behaviors) == 0 {
			continue
		}
		snake_actor := to_snake_case(a.name)
		defer delete(snake_actor)

		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintf(&sb, "// Generated Actor Bindings for `%s`\n", a.name)
		fmt.sbprintln(&sb, "// =============================================================================")
		fmt.sbprintln(&sb)

		// 1. Per-behavior message structs (and multi-return result tuple structs if needed)
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
			if len(b.result_fields) > 0 {
				fmt.sbprintf(&sb, "%s :: struct {{\n", b.result_struct)
				for rf in b.result_fields {
					fmt.sbprintf(&sb, "\t%s: %s,\n", rf.name, rf.type_expr)
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
			has_target := false
			for p in b.params {
				if p.name == "target" {
					has_target = true
					break
				}
			}
			target_arg := "_sleipnir_target" if has_target else "target"

			param_sig_b := strings.builder_make(context.temp_allocator)
			for p in b.params {
				wtype := p.type_expr
				if p.is_variadic && strings.has_prefix(wtype, "[]") {
					wtype = fmt.tprintf("..%s", wtype[2:])
				}
				if len(p.default_expr) > 0 {
					fmt.sbprintf(&param_sig_b, ", %s: %s = %s", p.name, wtype, p.default_expr)
				} else {
					fmt.sbprintf(&param_sig_b, ", %s: %s", p.name, wtype)
				}
			}
			param_sig := strings.to_string(param_sig_b)
			if len(b.return_type) > 0 {
				fmt.sbprintf(&sb, "\t%s: proc(%s: %s_Actor%s) -> %s,\n", b.short_name, target_arg, a.name, param_sig, b.return_type)
			} else {
				fmt.sbprintf(&sb, "\t%s: proc(%s: %s_Actor%s),\n", b.short_name, target_arg, a.name, param_sig)
			}
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_Actor :: struct {{\n", a.name)
		fmt.sbprintf(&sb, "\tusing raw:    ^%s.Actor(%s, %s_Msg),\n", a_alias, a.name, a.name)
		fmt.sbprintf(&sb, "\tusing vtable: ^%s_VTable,\n", a.name)
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "%s_PID :: %s.PID(%s_Msg)\n\n", a.name, a_alias, a.name)

		// 3. Generated `dispatch` procedure
		fmt.sbprintf(&sb, "%s_dispatch :: proc(ctx: ^%s.Context, self: ^%s, msg: ^%s_Msg) {{\n", snake_actor, a_alias, a.name, a.name)
		fmt.sbprintln(&sb, "\tswitch m in msg^ {")
		for b in a.behaviors {
			fmt.sbprintf(&sb, "\tcase %s_Msg_%s:\n", a.name, b.short_name)
			if len(b.result_fields) > 0 {
				fmt.sbprint(&sb, "\t\t")
				for _, ridx in b.result_fields {
					if ridx > 0 {
						fmt.sbprint(&sb, ", ")
					}
					fmt.sbprintf(&sb, "res_%d", ridx)
				}
				fmt.sbprintf(&sb, " := %s(", b.proc_name)
			} else if len(b.return_type) > 0 {
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
				if p.is_variadic {
					fmt.sbprintf(&sb, ", ..m.%s", p.name)
				} else {
					fmt.sbprintf(&sb, ", m.%s", p.name)
				}
			}
			fmt.sbprintln(&sb, ")")
			if len(b.result_fields) > 0 {
				fmt.sbprintf(&sb, "\t\t%s.reply(ctx, %s{{", a_alias, b.result_struct)
				for _, ridx in b.result_fields {
					if ridx > 0 {
						fmt.sbprint(&sb, ", ")
					}
					fmt.sbprintf(&sb, "res_%d", ridx)
				}
				fmt.sbprintln(&sb, "})")
			} else if len(b.return_type) > 0 {
				fmt.sbprintf(&sb, "\t\t%s.reply(ctx, res)\n", a_alias)
			}
		}
		fmt.sbprintln(&sb, "\t}")
		fmt.sbprintln(&sb, "}\n")

		// 4. Generated `send_<actor>_<behavior>` and `call_<actor>_<behavior>` procedures
		for b in a.behaviors {
			msg_struct_name := fmt.tprintf("%s_Msg_%s", a.name, b.short_name)

			has_target := false
			has_reply  := false
			for p in b.params {
				if p.name == "target" {
					has_target = true
				}
				if p.name == "reply_to" {
					has_reply = true
				}
			}
			target_arg := "_sleipnir_target" if has_target else "target"
			reply_arg  := "_sleipnir_reply_to" if has_reply else "reply_to"

			param_sig_b  := strings.builder_make(context.temp_allocator)
			no_def_sig_b := strings.builder_make(context.temp_allocator)
			lit_init_b   := strings.builder_make(context.temp_allocator)
			for p, idx in b.params {
				wtype := p.type_expr
				if p.is_variadic && strings.has_prefix(wtype, "[]") {
					wtype = fmt.tprintf("..%s", wtype[2:])
				}
				if len(p.default_expr) > 0 {
					fmt.sbprintf(&param_sig_b, ", %s: %s = %s", p.name, wtype, p.default_expr)
				} else {
					fmt.sbprintf(&param_sig_b, ", %s: %s", p.name, wtype)
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
				// 1) `call_<actor>_<behavior>_actor` & `_pid` (synchronous request/reply)
				call_actor_fn := fmt.tprintf("call_%s_%s_actor", snake_actor, b.short_name)
				call_pid_fn   := fmt.tprintf("call_%s_%s_pid", snake_actor, b.short_name)
				call_res_type := b.result_struct if len(b.result_fields) > 0 else b.return_type

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s_Actor%s) -> %s {{\n", call_actor_fn, target_arg, a.name, param_sig, b.return_type)
				if len(b.result_fields) > 0 {
					fmt.sbprintf(&sb, "\tres := %s.call(%s.raw, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, call_res_type)
					fmt.sbprint(&sb, "\treturn ")
					for rf, ridx in b.result_fields {
						if ridx > 0 {
							fmt.sbprint(&sb, ", ")
						}
						fmt.sbprintf(&sb, "res.%s", rf.name)
					}
					fmt.sbprintln(&sb)
				} else {
					fmt.sbprintf(&sb, "\treturn %s.call(%s.raw, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, call_res_type)
				}
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s.PID(%s_Msg)%s) -> %s {{\n", call_pid_fn, target_arg, a_alias, a.name, param_sig, b.return_type)
				if len(b.result_fields) > 0 {
					fmt.sbprintf(&sb, "\tres := %s.call(%s, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, call_res_type)
					fmt.sbprint(&sb, "\treturn ")
					for rf, ridx in b.result_fields {
						if ridx > 0 {
							fmt.sbprint(&sb, ", ")
						}
						fmt.sbprintf(&sb, "res.%s", rf.name)
					}
					fmt.sbprintln(&sb)
				} else {
					fmt.sbprintf(&sb, "\treturn %s.call(%s, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, call_res_type)
				}
				fmt.sbprintln(&sb, "}\n")

				add_pair_to_group(&call_groups, b, call_actor_fn, call_pid_fn)

				// 2) `send_<actor>_<behavior>_reply_actor` & `_reply_pid` (async forward of return value to a `reply_to` PID)
				send_reply_actor_fn := fmt.tprintf("send_%s_%s_reply_actor", snake_actor, b.short_name)
				send_reply_pid_fn   := fmt.tprintf("send_%s_%s_reply_pid", snake_actor, b.short_name)

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s_Actor%s, %s: %s.PID(%s)) {{\n", send_reply_actor_fn, target_arg, a.name, no_def_sig, reply_arg, a_alias, call_res_type)
				fmt.sbprintf(&sb, "\t%s.send(%s.raw, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, reply_arg)
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s.PID(%s_Msg)%s, %s: %s.PID(%s)) {{\n", send_reply_pid_fn, target_arg, a_alias, a.name, no_def_sig, reply_arg, a_alias, call_res_type)
				fmt.sbprintf(&sb, "\t%s.send(%s, %s_Msg(%s{{%s}}), %s)\n", a_alias, target_arg, a.name, msg_struct_name, lit_init, reply_arg)
				fmt.sbprintln(&sb, "}\n")

				add_pair_to_group(&send_groups, b, send_reply_actor_fn, send_reply_pid_fn)
			} else {
				// 1-way fire-and-forget behavior (`send_<name>`)
				actor_fn := fmt.tprintf("send_%s_%s_actor", snake_actor, b.short_name)
				pid_fn   := fmt.tprintf("send_%s_%s_pid", snake_actor, b.short_name)

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s_Actor%s) {{\n", actor_fn, target_arg, a.name, param_sig)
				fmt.sbprintf(&sb, "\t%s.send(%s.raw, %s_Msg(%s{{%s}}))\n", a_alias, target_arg, a.name, msg_struct_name, lit_init)
				fmt.sbprintln(&sb, "}\n")

				fmt.sbprintf(&sb, "%s :: #force_inline proc(%s: %s.PID(%s_Msg)%s) {{\n", pid_fn, target_arg, a_alias, a.name, param_sig)
				fmt.sbprintf(&sb, "\t%s.send(%s, %s_Msg(%s{{%s}}))\n", a_alias, target_arg, a.name, msg_struct_name, lit_init)
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
		fmt.sbprintf(&sb, "\ta, _ := %s.spawn(initial_state, %s_dispatch, id, allocator)\n", a_alias, snake_actor)
		fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_actor)
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintf(&sb, "spawn_%s_sched :: proc(\n", snake_actor)
		fmt.sbprintf(&sb, "\ts: ^%s.Scheduler,\n", a_alias)
		fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
		fmt.sbprintln(&sb, "\tid: u32 = 0,")
		fmt.sbprintln(&sb, "\tallocator := context.allocator,")
		fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
		fmt.sbprintf(&sb, "\ta, _ := %s.spawn(s, initial_state, %s_dispatch, id, allocator)\n", a_alias, snake_actor)
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
			for b in pending {
				delete(b.params)
				delete(b.result_fields)
			}
			return "", false
		}
	}
	finalize_module_spec(&mod, pending[:])
	return emit_module_code(&mod, allocator), true
}

// Validate a parsed `Module_Spec` for invalid `@(supervise)` / `@(reinit)` attributes
// or ambiguous RPC reply signatures. Stubbed in Track 2; implemented in Track 4.
validate_module_spec :: proc(
	spec: ^Module_Spec,
	allocator := context.allocator,
) -> (diags: []Diagnostic, ok: bool) {
	_ = spec
	_ = allocator
	return nil, true
}

// High-level helper: parse a single Odin source string and validate its module specification.
validate_source :: proc(
	src: string,
	filename: string = "input.odin",
	allocator := context.allocator,
) -> (diags: []Diagnostic, ok: bool) {
	mod: Module_Spec
	init_module_spec(&mod)
	defer destroy_module_spec(&mod)

	pending := make([dynamic]Behavior_Spec)
	defer delete(pending)

	if !parse_source_into_spec(src, filename, &mod, &pending) {
		for b in pending {
			delete(b.params)
			delete(b.result_fields)
		}
		out := make([]Diagnostic, 1, allocator)
		out[0] = Diagnostic{
			file    = filename,
			line    = 1,
			message = "syntax/parse error",
		}
		return out, false
	}
	finalize_module_spec(&mod, pending[:])
	return validate_module_spec(&mod, allocator)
}

// Scan a directory of `.odin` files (skipping `*_gen.odin`, `*_test.odin`, and mismatched OS/build files),
// parse all `@(actor)` and behavior declarations across all files in the package, and write `<dir>/actors_gen.odin`.
generate_directory :: proc(dir_path: string, out_filename: string = "actors_gen.odin") -> (out_path: string, ok: bool) {
	f, err := os.open(dir_path)
	if err != nil {
		return "", false
	}
	defer os.close(f)

	entries, rerr := os.read_all_directory(f, context.allocator)
	if rerr != nil {
		return "", false
	}
	defer os.file_info_slice_delete(entries, context.allocator)

	filenames := make([dynamic]string)
	defer delete(filenames)
	for entry in entries {
		if entry.type == .Regular &&
		   strings.has_suffix(entry.name, ".odin") &&
		   !strings.has_suffix(entry.name, "_gen.odin") &&
		   !strings.has_suffix(entry.name, "_test.odin") &&
		   matches_filename_os(entry.name) &&
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
			for b in pending {
				delete(b.params)
				delete(b.result_fields)
			}
			return "", false
		}
		append(&file_buffers, data)
		src := string(data)
		if !matches_build_tags(src) {
			continue
		}
		if !parse_source_into_spec(src, full, &mod, &pending) {
			for b in pending {
				delete(b.params)
				delete(b.result_fields)
			}
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
