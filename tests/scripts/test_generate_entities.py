"""
Unit tests for scripts/generate_entities.py (the @relais entity generator).

Each test writes headers to a scratch directory, then either parses them in
process (StructParser) or runs the generator as a subprocess to observe its
exit code, its diagnostics and the files it writes.

Run: python3 -m unittest discover -s tests/scripts
"""

import importlib.util
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "scripts" / "generate_entities.py"


def _load_generator():
    spec = importlib.util.spec_from_file_location("generate_entities", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gen = _load_generator()


class GeneratorCase(unittest.TestCase):
    """Scratch source and output directories, and helpers to parse or generate."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.src = root / "src"
        self.out = root / "out"
        self.src.mkdir()

    def tearDown(self):
        self._tmp.cleanup()

    def header(self, name: str, body: str) -> Path:
        path = self.src / name
        path.write_text(textwrap.dedent(body).lstrip("\n"))
        return path

    def parse(self, body: str) -> list:
        parser = gen.StructParser()
        entities = parser.parse_file(self.header("Entity.h", body))
        self.assertEqual([str(d) for d in parser.errors], [])
        return entities

    def entity(self, body: str):
        entities = self.parse(body)
        self.assertEqual(len(entities), 1)
        return entities[0]

    def member(self, line: str, type_and_name: str = "int32_t views = 0;"):
        """Parse one annotated member of a minimal entity and return it."""
        e = self.entity(f"""
            // @relais table=t
            struct T {{
                int64_t id = 0;  // @relais primary_key db_managed
                {type_and_name}  {line}
            }};
        """)
        return e.members[1]

    def run_generator(self, *sources: Path) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(GENERATOR),
             "--sources", *(str(s) for s in (sources or (self.src,))),
             "--output-dir", str(self.out)],
            capture_output=True, text=True)


class MemberParsing(GeneratorCase):

    def test_members_in_declaration_order(self):
        e = self.entity("""
            // @relais table=users
            struct User {
                int64_t id = 0;               // @relais primary_key db_managed
                std::string name;
                std::optional<int32_t> score;
            };
        """)
        self.assertEqual([m.name for m in e.members], ["id", "name", "score"])
        self.assertEqual([m.cpp_type for m in e.members],
                         ["int64_t", "std::string", "std::optional<int32_t>"])
        self.assertEqual(e.members[0].tags, {"primary_key", "db_managed"})
        self.assertTrue(e.members[2].is_optional)

    def test_column_renames_the_database_column(self):
        m = self.member("// @relais column=mail", "std::string email;")
        self.assertEqual(m.name, "email")
        self.assertEqual(m.col_name, "mail")

    def test_private_and_protected_members_are_not_columns(self):
        e = self.entity("""
            // @relais table=t
            class T {
            public:
                int64_t id = 0;  // @relais primary_key
            protected:
                int32_t hint_ = 0;
            public:
                std::string label;
            private:
                int32_t cache_ = 0;
            };
        """)
        self.assertEqual([m.name for m in e.members], ["id", "label"])

    def test_methods_and_aliases_are_not_columns(self):
        e = self.entity("""
            // @relais table=t
            struct T {
                using Id = int64_t;
                int64_t id = 0;  // @relais primary_key
                [[nodiscard]] bool empty() const { return id == 0; }
                int32_t twice() const;
                std::string label;
            };
        """)
        self.assertEqual([m.name for m in e.members], ["id", "label"])

    def test_prose_before_the_marker_is_ignored(self):
        m = self.member("// assigned by the caller — @relais primary_key",
                        "int64_t key_id = 0;")
        self.assertEqual(m.tags, {"primary_key"})


class CodeStripping(unittest.TestCase):
    """_strip_code: the code of a line, comments and literal contents blanked."""

    def strip(self, line: str, state: str = "") -> tuple[str, str]:
        return gen.StructParser._strip_code(line, state)

    def test_comments(self):
        self.assertEqual(self.strip("int a = 0;  // }"), ("int a = 0;  ", ""))
        self.assertEqual(self.strip("int a; /* { */ int b;"), ("int a;         int b;", ""))
        self.assertEqual(self.strip("// @relais primary_key"), ("", ""))

    def test_block_comment_across_lines(self):
        self.assertEqual(self.strip("int a; /* opens {"), ("int a;   ", "*/"))
        self.assertEqual(self.strip("   } still inside", "*/"), ("", "*/"))
        self.assertEqual(self.strip("   ends */ int b;", "*/"), ("           int b;", ""))

    def test_string_and_char_literals(self):
        self.assertEqual(self.strip('std::string s = "a{b\\"}";'), ('std::string s = "      ";', ""))
        self.assertEqual(self.strip("char c = '}';"), ("char c = ' ';", ""))
        self.assertEqual(self.strip("char c = '\\'';"), ("char c = '  ';", ""))
        self.assertEqual(self.strip('auto s = u8"{";'), ('auto s = u8" ";', ""))
        self.assertEqual(self.strip('auto s = "// x";'), ('auto s = "    ";', ""))

    def test_digit_separators_are_not_char_literals(self):
        line = "std::array<int64_t, 2> a = {1'000'000, 0xF'FF'F};"
        self.assertEqual(self.strip(line), (line, ""))

    def test_raw_strings(self):
        self.assertEqual(self.strip('auto r = R"x(a } )" b)x";'),
                         ('auto r = R"            ";', ""))
        self.assertEqual(self.strip('auto r = R"(opens {'), ('auto r = R" ', ')"'))
        self.assertEqual(self.strip('  } )" ;', ')"'), ('     " ;', ""))


class BraceCounting(GeneratorCase):
    """Braces in comments and literals do not delimit the struct body."""

    def names(self, body: str) -> list:
        return [(e.class_name, [m.name for m in e.members]) for e in self.parse(body)]

    def test_closing_brace_in_a_comment(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;     // @relais primary_key
                std::string title;  // shown as "{title}"
                std::string body;   // ends with }
                int32_t views = 0;
            };
        """), [("T", ["id", "title", "body", "views"])])

    def test_opening_brace_in_a_comment(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;     // @relais primary_key
                std::string label;  // format { key: value
                int32_t views = 0;
            };

            struct Internal {
                std::string secret;
            };
        """), [("T", ["id", "label", "views"])])

    def test_block_comment_spanning_lines(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                /* legacy layout: {
                       name, }} */
                std::string name;
                int32_t views = 0;
            };
        """), [("T", ["id", "name", "views"])])

    def test_braces_in_literals(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                std::string open = "{";
                char close = '}';
                static constexpr const char* kQuoted = R"(say "}")";
                int32_t views = 0;
            };
        """), [("T", ["id", "open", "close", "views"])])

    def test_a_struct_right_after_the_entity_is_not_absorbed(self):
        self.assertEqual(self.names("""
            // @relais table=a
            struct A {
                int64_t id = 0;  // @relais primary_key
            };
            struct Helper {
                int32_t leaked = 0;
            };
        """), [("A", ["id"])])

    def test_struct_and_namespace_closed_on_one_line(self):
        self.assertEqual(self.names("""
            namespace ns {
            // @relais table=a
            struct A {
                int64_t id = 0;  // @relais primary_key
            }; }

                inline constexpr int32_t leaked = 0;
        """), [("A", ["id"])])

    def test_the_word_struct_in_a_comment_is_not_a_declaration(self):
        self.assertEqual(self.names("""
            // @relais table=t
            // This struct maps the t table.
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """), [("T", ["id"])])

    def test_a_struct_never_closed_is_rejected(self):
        self.header("Open.h", """
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
        """)
        r = self.run_generator()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("never closed", r.stderr)
        self.assertFalse(self.out.exists() and any(self.out.iterdir()))


class BodyDepth(GeneratorCase):
    """A data member is a declaration of the struct body itself (depth 1)."""

    def names(self, body: str) -> list:
        return [m.name for m in self.entity(body).members]

    def test_opening_brace_on_its_own_line(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T
            {
                int64_t id = 0;  // @relais primary_key
                std::string name;
            };
        """), ["id", "name"])

    def test_brace_initialized_members(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id{0};  // @relais primary_key
                std::vector<int32_t> ranks{1, 2};
                int32_t views{};
            };
        """), ["id", "ranks", "views"])

    def test_initializers_with_parentheses(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                std::string name = std::string("x");
                std::string tpl = R"({"a": })";
            };
        """), ["id", "name", "tpl"])

    def test_function_bodies_hold_no_member(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                int32_t views = 0;
                int32_t twice() const {
                    int32_t tmp = views * 2;
                    return tmp;
                }
            };
        """), ["id", "views"])

    def test_nested_type_members_stay_in_the_nested_type(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                struct Inner {
                    int32_t hidden = 0;
                };
                Inner inner;
            };
        """), ["id", "inner"])

    def test_struct_opened_after_other_code_on_its_line(self):
        self.assertEqual(self.names("""
            // @relais table=t
            namespace app { struct T {
                int64_t id = 0;  // @relais primary_key
                std::string name;
            }; }
        """), ["id", "name"])

    def test_access_specifier_followed_by_a_comment(self):
        self.assertEqual(self.names("""
            // @relais table=t
            class T {
                int32_t secret_ = 0;
            public:  // columns
                int64_t id = 0;  // @relais primary_key
            private:  // cache
                int32_t hits_ = 0;
            };
        """), ["id"])

    def test_declarations_that_are_not_data_members(self):
        self.assertEqual(self.names("""
            // @relais table=t
            struct T {
                static constexpr int32_t kMax = 5;
                inline static int32_t instances = 0;
                using Id = int64_t;
                typedef int64_t Key;
                friend class Codec;
                struct Forward;
                enum class Kind { A, B };
                template <typename X> using List = std::vector<X>;
                int64_t id = 0;  // @relais primary_key
            };
        """), ["id"])

    def test_an_annotated_declaration_without_definition_is_rejected(self):
        self.header("Fwd.h", """
            // @relais table=t
            struct T;

            struct Other {
                int64_t id = 0;
            };
        """)
        r = self.run_generator()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("not defined", r.stderr)


class EntityAnnotations(GeneratorCase):

    def test_table_and_namespace(self):
        e = self.entity("""
            namespace app::model {
            // @relais table=app_users
            struct User {
                int64_t id = 0;  // @relais primary_key db_managed
            };
            }
        """)
        self.assertEqual(e.class_name, "User")
        self.assertEqual(e.namespace, "app::model")
        self.assertEqual(e.annotation.table, "app_users")
        self.assertEqual(e.annotation.entity_fqn, "app::model::User")

    def test_read_only(self):
        for spelling in ("read_only", "read_only=true"):
            with self.subTest(spelling=spelling):
                e = self.entity(f"""
                    // @relais table=v {spelling}
                    struct V {{
                        int64_t id = 0;  // @relais primary_key
                    }};
                """)
                self.assertTrue(e.annotation.read_only)

    def test_composite_and_partition_keys(self):
        e = self.entity("""
            // @relais table=t
            struct T {
                int64_t a = 0;       // @relais primary_key
                int64_t b = 0;       // @relais primary_key
                std::string region;  // @relais partition_key
            };
        """)
        self.assertEqual(e.annotation.primary_keys, ["a", "b"])
        self.assertTrue(e.annotation.is_composite)
        self.assertEqual(e.annotation.partition_keys, ["region"])

    def test_tags_separated_by_any_whitespace(self):
        for gap in ("  ", "\t"):
            with self.subTest(gap=gap):
                e = self.entity(f"""
                    // @relais table=v{gap}read_only
                    struct V {{
                        int64_t id = 0;  // @relais primary_key
                    }};
                """)
                self.assertEqual(e.annotation.table, "v")
                self.assertTrue(e.annotation.read_only)

    def test_list_limits_are_sorted_and_deduplicated(self):
        e = self.entity("""
            // @relais table=t
            // @relais_list limits=50,10,25,10
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """)
        self.assertEqual(e.annotation.limits, [10, 25, 50])
        self.assertTrue(e.annotation.has_list)

    def test_list_entity_overrides_the_descriptor_name(self):
        e = self.entity("""
            // @relais table=t
            // @relais_list limits=10 entity=other::Name
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """)
        self.assertEqual(e.annotation.entity_fqn, "other::Name")

    def test_two_entities_in_one_header(self):
        entities = self.parse("""
            // @relais table=a
            struct A {
                int64_t id = 0;  // @relais primary_key
                std::string x;
            };

            // @relais table=b
            struct B {
                int64_t id = 0;  // @relais primary_key
                std::string y;
            };
        """)
        self.assertEqual([(e.class_name, [m.name for m in e.members]) for e in entities],
                         [("A", ["id", "x"]), ("B", ["id", "y"])])


class FilterAndSortTags(GeneratorCase):

    def test_filterable_forms(self):
        cases = {
            "filterable":                ("views", "eq"),
            "filterable:ge":             ("views", "ge"),
            "filterable:gte":            ("views", "ge"),
            "filterable:lte":            ("views", "le"),
            "filterable:in":             ("views", "in"),
            "filterable:nin":            ("views", "nin"),
            "filterable:not_in":         ("views", "nin"),
            "filterable:min_views":      ("min_views", "eq"),
            "filterable:min_views:ge":   ("min_views", "ge"),
            "filterable:vs:not_in":      ("vs", "nin"),
        }
        for tag, (param, op) in cases.items():
            with self.subTest(tag=tag):
                m = self.member(f"// @relais {tag}")
                self.assertEqual([(f.param, f.field, f.op) for f in m.filter_configs],
                                 [(param, "views", op)])

    def test_is_null_on_an_optional_member(self):
        m = self.member("// @relais filterable:no_date:is_null",
                        "std::optional<std::string> published_at;")
        self.assertEqual([(f.param, f.op) for f in m.filter_configs],
                         [("no_date", "is_null")])

    def test_sortable_forms(self):
        cases = {
            "sortable":            ("views", "desc"),
            "sortable:asc":        ("views", "asc"),
            "sortable:desc":       ("views", "desc"),
            "sortable:by_views":   ("by_views", "desc"),
            "sortable:hot:asc":    ("hot", "asc"),
        }
        for tag, (param, direction) in cases.items():
            with self.subTest(tag=tag):
                m = self.member(f"// @relais {tag}")
                self.assertEqual([(s.param, s.field, s.direction) for s in m.sort_configs],
                                 [(param, "views", direction)])

    def test_several_tags_on_one_member(self):
        m = self.member("// @relais filterable filterable:min:ge sortable:asc")
        self.assertEqual([(f.param, f.op) for f in m.filter_configs],
                         [("views", "eq"), ("min", "ge")])
        self.assertEqual([(s.param, s.direction) for s in m.sort_configs],
                         [("views", "asc")])


class EnumTags(GeneratorCase):

    def test_explicit_enum_mapping(self):
        e = self.entity("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                State state = State::Open;  // @relais enum=open:Open,closed:Closed
            };
        """)
        self.assertEqual([(en.field_name, en.pairs, en.cpp_type) for en in e.annotation.enums],
                         [("state", [("open", "Open"), ("closed", "Closed")], "State")])

    def test_bare_enum_reads_the_mapping_from_glaze(self):
        m = self.member("// @relais enum", "State state = State::Open;")
        self.assertIn("auto_enum", m.tags)


class GeneratorRun(GeneratorCase):

    def test_writes_one_header_per_entity(self):
        self.header("A.h", """
            // @relais table=a
            struct A {
                int64_t id = 0;  // @relais primary_key db_managed
                std::string name;
            };
        """)
        self.header("B.h", """
            // @relais table=b
            struct B {
                int64_t id = 0;  // @relais primary_key db_managed
            };
        """)
        r = self.run_generator()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(sorted(p.name for p in self.out.iterdir()),
                         ["AEntity.h", "BEntity.h"])
        self.assertIn('"SELECT id, name FROM a WHERE id = $1"',
                      (self.out / "AEntity.h").read_text())

    def test_the_test_fixtures_generate(self):
        r = self.run_generator(ROOT / "tests" / "fixtures")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue((self.out / "TestArticleEntity.h").is_file())


class Rejections(GeneratorCase):
    """Annotations the generator already refuses: exit code 1 and a message."""

    def assertRejected(self, body: str, message: str):
        self.header("Bad.h", body)
        r = self.run_generator()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn(message, r.stderr)

    def test_limits_out_of_range(self):
        self.assertRejected("""
            // @relais table=t
            // @relais_list limits=0,10
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """, "out of range")

    def test_limits_not_an_integer(self):
        self.assertRejected("""
            // @relais table=t
            // @relais_list limits=ten
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """, "not an integer")

    def test_is_null_on_a_member_that_cannot_be_null(self):
        self.assertRejected("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                int64_t author_id = 0;  // @relais filterable:no_author:is_null
            };
        """, "is_null requires a std::optional member")

    def test_a_filter_parameter_declared_twice(self):
        self.assertRejected("""
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                int32_t a = 0;  // @relais filterable:x
                int32_t b = 0;  // @relais filterable:x
            };
        """, "filter parameter 'x' is declared twice")


class Diagnostics(GeneratorCase):
    """Every error of a run, each as path:line:column: error: message."""

    def errors(self, *sources: Path) -> list:
        r = self.run_generator(*sources)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        return r.stderr.splitlines()

    def test_errors_point_at_the_faulty_token(self):
        bad = self.header("Bad.h", """
            // @relais table=t
            // @relais_list limits=10,0
            struct T {
                int64_t id = 0;  // @relais primary_key
                int32_t a = 0;   // @relais filterable:x
                int32_t b = 0;   // @relais sortable filterable:x
                std::string c;   // @relais filterable:none:is_null
            };
        """)
        self.assertEqual(self.errors()[:3], [
            f"{bad}:2:27: error: limits= value 0 out of range [1, 65535]",
            f"{bad}:6:42: error: filter parameter 'x' is declared twice; "
            f"name one of them (filterable:<param>:<op>)",
            f"{bad}:7:33: error: is_null requires a std::optional member; "
            f"'c' is std::string",
        ])

    def test_structural_errors_point_at_the_struct_name(self):
        fwd = self.header("Fwd.h", """
            // @relais table=t
            struct T;
        """)
        open_ = self.header("Open.h", """
            // @relais table=o
            struct Open {
                int64_t id = 0;  // @relais primary_key
        """)
        errors = self.errors()
        self.assertTrue(errors[0].startswith(f"{fwd}:2:8: error: 'T' is declared but not defined"),
                        errors)
        self.assertTrue(errors[1].startswith(f"{open_}:2:8: error: the body of 'Open' is never closed"),
                        errors)

    def test_every_error_of_the_run_is_reported(self):
        a = self.header("A.h", """
            // @relais table=a
            // @relais_list limits=ten
            struct A {
                int64_t id = 0;  // @relais primary_key
            };
        """)
        b = self.header("B.h", """
            // @relais table=b
            struct B {
                int64_t id = 0;  // @relais primary_key filterable:id:is_null
            };
        """)
        missing = self.src / "Missing.h"
        errors = self.errors(missing, self.src)
        self.assertEqual([e.split(": error: ")[0] for e in errors[:-1]],
                         [str(missing), f"{a}:2:24", f"{b}:3:45"])
        self.assertEqual(errors[-1], "3 errors, no file written")

    def test_an_entity_with_an_error_is_left_out(self):
        parser = gen.StructParser()
        entities = parser.parse_file(self.header("Two.h", """
            // @relais table=a
            // @relais_list limits=0
            struct A {
                int64_t id = 0;  // @relais primary_key
            };

            // @relais table=b
            struct B {
                int64_t id = 0;  // @relais primary_key
            };
        """))
        self.assertEqual([e.class_name for e in entities], ["B"])
        self.assertEqual(len(parser.errors), 1)

    def test_an_unreadable_header(self):
        link = self.src / "Gone.h"
        link.symlink_to(self.src / "nowhere.h")
        self.assertEqual(self.errors()[0].split(": error: ")[0], str(link))

    def test_an_error_found_while_generating(self):
        bad = self.header("Bad.h", """
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                State state = State::Open;  // @relais enum
            };
        """)
        self.assertTrue(self.errors()[0].startswith(
            f"{bad}:4:44: error: @relais enum on field 'state': no glz::meta<State>"))

    def test_a_name_collision_with_an_entity_that_fails(self):
        (self.src / "a").mkdir()
        (self.src / "b").mkdir()
        first = self.header("a/T.h", """
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
                State state = State::Open;  // @relais enum
            };
        """)
        second = self.header("b/T.h", """
            // @relais table=t
            struct T {
                int64_t id = 0;  // @relais primary_key
            };
        """)
        self.assertEqual([e.split(": ")[:2] for e in self.errors()[:-1]],
                         [[f"{first}:4:44", "error"],
                          [f"{second}:2:8", "error"],
                          [f"{first}:2:8", "note"]])


class SafeOutput(GeneratorCase):
    """A failed run writes nothing; one generated header per entity name."""

    VALID = """
        // @relais table={t}
        struct {t} {{
            int64_t id = 0;  // @relais primary_key db_managed
        }};
    """

    def setUp(self):
        super().setUp()
        self.out.mkdir()
        self.kept = self.out / "Kept.h"
        self.kept.write_text("previous run\n")
        self.kept_mtime = self.kept.stat().st_mtime_ns

    def assertNothingWritten(self, r: subprocess.CompletedProcess):
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertEqual([p.name for p in self.out.iterdir()], ["Kept.h"])
        self.assertEqual(self.kept.read_text(), "previous run\n")
        self.assertEqual(self.kept.stat().st_mtime_ns, self.kept_mtime)

    def test_a_parse_error_after_valid_entities(self):
        a = self.header("A.h", self.VALID.format(t="A"))
        b = self.header("B.h", """
            // @relais table=b
            // @relais_list limits=0
            struct B {
                int64_t id = 0;  // @relais primary_key
            };
        """)
        c = self.header("C.h", self.VALID.format(t="C"))
        self.assertNothingWritten(self.run_generator(a, b, c))

    def test_a_generation_error_after_valid_entities(self):
        a = self.header("A.h", self.VALID.format(t="A"))
        b = self.header("B.h", """
            // @relais table=b
            struct B {
                int64_t id = 0;  // @relais primary_key
                State state = State::Open;  // @relais enum
            };
        """)
        self.assertNothingWritten(self.run_generator(a, b))

    def test_two_entities_generating_the_same_header(self):
        (self.src / "admin").mkdir()
        (self.src / "app").mkdir()
        first = self.header("admin/User.h", """
            namespace admin {
            // @relais table=admins
            struct User {
                int64_t id = 0;  // @relais primary_key
            };
            }
        """)
        second = self.header("app/User.h", """
            namespace app {
            // @relais table=users
            struct User {
                int64_t id = 0;  // @relais primary_key
            };
            }
        """)
        r = self.run_generator()
        self.assertNothingWritten(r)
        self.assertEqual(r.stderr.splitlines(), [
            f"{second}:3:8: error: 'app::User' generates UserEntity.h, as does 'admin::User'",
            f"{first}:3:8: note: 'admin::User' is declared here",
            "1 error, no file written",
        ])

    def test_a_header_given_twice_is_generated_once(self):
        self.header("A.h", self.VALID.format(t="A"))
        (self.src / "sub").mkdir()
        again = self.src / "sub" / ".."
        r = self.run_generator(again, again / "A.h")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(sorted(p.name for p in self.out.iterdir()), ["AEntity.h", "Kept.h"])


if __name__ == "__main__":
    unittest.main()
