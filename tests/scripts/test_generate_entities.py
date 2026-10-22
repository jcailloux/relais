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
        return gen.StructParser().parse_file(self.header("Entity.h", body))

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


if __name__ == "__main__":
    unittest.main()
