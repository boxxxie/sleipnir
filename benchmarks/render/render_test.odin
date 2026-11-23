package bench_render

import "core:strings"
import "core:testing"

@(test)
test_render_report_and_line_chart :: proc(t: ^testing.T) {
	records := [?]Hugi_Record{
		{commit = "6365481", group_name = "workload_1_fan_in", name = "3e_sharded_spsc_fan_in", full_name = "workload_1_fan_in/3e_sharded_spsc_fan_in", samples = 35, p50_ns = 0.77, p95_ns = 1.0, cv_pct = 3.0, ops_per_sec = 1_290_000_000, allocs_per_op = 0, alloc_bytes_per_op = 0},
		{commit = "6365481", group_name = "workload_5_a2a", name = "5c_actor_a2a_1w", full_name = "workload_5_a2a/5c_actor_a2a_1w", samples = 35, p50_ns = 197.14, p95_ns = 210.0, cv_pct = 2.0, ops_per_sec = 5_072_000, allocs_per_op = 0, alloc_bytes_per_op = 0},
		{commit = "6365481", group_name = "workload_5_a2a", name = "5c_actor_a2a_16w", full_name = "workload_5_a2a/5c_actor_a2a_16w", samples = 35, p50_ns = 40.64, p95_ns = 45.0, cv_pct = 2.5, ops_per_sec = 24_600_000, allocs_per_op = 0, alloc_bytes_per_op = 0},
	}

	rep := build_suite_report_view(records[:], "6365481", context.temp_allocator)
	testing.expect_value(t, rep.total_benchmarks, 3)
	testing.expect_value(t, rep.group_count, 2)

	sb := strings.builder_make(0, 1024, context.temp_allocator)
	_, err := suite_report_tmpl.with(strings.to_writer(&sb), rep)
	testing.expect(t, err == nil, "suite_report template renders without error")
	out := strings.to_string(sb)
	testing.expect(t, strings.contains(out, "| `3e_sharded_spsc_fan_in` |"), "strips group/ prefix cleanly")

	x := [2]string{"1w", "16w"}
	vals := [2]Maybe(f64){197.14, 40.64}
	lc, ok := build_line_chart_view("A2A", "sub", x[:], {{"Sleipnir", vals[:], PALETTE[1]}}, false, "ns", context.temp_allocator)
	testing.expect(t, ok, "line chart builds cleanly")
	lsb := strings.builder_make(0, 1024, context.temp_allocator)
	_, lerr := line_chart_tmpl.with(strings.to_writer(&lsb), lc)
	testing.expect(t, lerr == nil, "line chart template renders without error")
	free_all(context.temp_allocator)
}
