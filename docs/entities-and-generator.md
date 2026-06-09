# Entities & the code generator

An **entity** in relais is two decoupled pieces:

1. A **pure C++ struct** — framework-agnostic data, shareable across projects.
2. A generated **Mapping** — ORM glue (SQL strings, `fromRow`, `toInsertParams`,
   `key`, `TraitsType`, list descriptor) derived from `@relais` annotations.

At the API layer `Entity<Struct, Mapping>` fuses the two and adds thread-safe
lazy BEVE/JSON serialization. Repositories only ever speak `Entity`.

This page covers writing the struct, the annotation language, what the generator
emits (and **where** — the namespace matters), and how to wire it into CMake.

## 1. Define the struct

```cpp
// entities/User.h — pure data, no relais dependency
struct User {
    int64_t id = 0;            // @relais primary_key db_managed
    std::string username;
    std::string email;
    int32_t balance = 0;
    std::string created_at;    // @relais timestamp
};
```

Struct-level annotations go on comment lines **immediately above** the
`struct`; field-level annotations go in a trailing `// @relais ...` comment on
the member line.

```cpp
// @relais table=users
struct User { ... };
```

## 2. Run the generator

```bash
python scripts/generate_entities.py --sources entities/ --output-dir entities/generated/
```

- `--sources` accepts a mix of directories (scanned recursively for `*.h`,
  skipping anything under a `generated/` path or named `*Wrapper*`) and explicit
  files.
- `--output-dir` is the **only** thing that controls where files land. Each
  entity is written to `{ClassName}Entity.h` in that directory — e.g.
  `User` → `entities/generated/UserEntity.h`.

> **There is no per-entity output path.** Some older examples showed a
> `@relais output=...` annotation — it is **not parsed** and has no effect.
> Output location is governed entirely by `--output-dir`.

### Generated-file include path is **relative**

The generated header re-includes its source struct with a path computed
*relative to the output directory*:

```cpp
// entities/generated/UserEntity.h
#include "../User.h"        // os.path.relpath(source, output_dir)
```

**Keep `SOURCES` and `OUTPUT_DIR` in a stable directory relationship.**
Generating into an unrelated tree (e.g. `/tmp`) yields a broken walk like
`../../../home/you/entities/User.h`. The safe pattern is a `generated/`
subfolder next to the sources:

```
entities/
├── User.h
└── generated/
    └── UserEntity.h     →  #include "../User.h"
```

## 3. Use the generated type — mind the namespace

Everything the generator emits lives in **`namespace entity::generated`** —
both the Mapping *and* the `Entity` alias:

```cpp
// entities/generated/UserEntity.h (generated)
namespace entity::generated {

struct UserMapping { /* SQL, fromRow, TraitsType, ... */ };

using UserEntity = jcailloux::relais::Entity<::User, UserMapping>;

}  // namespace entity::generated
```

So the fully-qualified names a consumer references are
`entity::generated::UserEntity` and `entity::generated::UserMapping` — **not**
`generated::UserMapping` relative to your own namespace, and the alias does
**not** land in your namespace. Import it explicitly:

```cpp
#include <jcailloux/relais/repository/Repo.h>
#include "entities/generated/UserEntity.h"

namespace myapp {
using entity::generated::UserEntity;          // bring the alias in

using UserRepo = jcailloux::relais::Repo<UserEntity, "User">;
}
```

The generator also emits, **at global scope** (outside `entity::generated`):

- `glz::meta<::User>` — *only if the source header doesn't already define one*
  (see [Custom JSON field names](#custom-json-field-names)).
- `glz::meta<UserMapping::RowView>` — drives zero-copy row→JSON/BEVE.

## Annotation reference

### Struct-level

| Annotation | Effect |
|---|---|
| `@relais table=users` | PostgreSQL table name (required; otherwise derived from the class name with a warning). |
| `@relais read_only` | Marks the entity read-only — no `toInsertParams`, no `update`, no `Field` enum. |
| `@relais_list limits=10,25,50` | Pagination limits for a list entity. First = `defaultLimit`, last = `maxLimit`. |

### Field-level

| Annotation | Effect |
|---|---|
| `primary_key` | Marks the primary key. Repeat across fields for a composite key. |
| `db_managed` | Excluded from `INSERT` (DB-generated, e.g. serial id, default timestamp). |
| `timestamp` | Stored as `std::string` (ISO 8601). |
| `nullable` | `std::optional<T>` handling, `setNull` support in `patch`. |
| `column=db_name` | Override the DB column name (defaults to the field name). |
| `raw_json` | `glz::raw_json_t` — stored verbatim as a string column. |
| `json_field` | Struct/vector serialized to/from a JSON column. |
| `enum` | Auto-resolve the DB↔enum mapping from `glz::meta<EnumType>` in the source header. |
| `enum=db1:Variant1,db2:Variant2` | Explicit DB↔enum mapping (overrides `glz::meta`). |
| `partition_key` | Partition column — enables single-partition DELETE pruning (see [caching.md](caching.md#partition-key-repositories)). |
| `filterable[...]` | List filter — see [lists.md](lists.md). |
| `sortable[...]` | List sort — see [lists.md](lists.md). |

## Custom JSON field names

By default JSON/BEVE field names equal the C++ member names. To use different
names (e.g. camelCase for a REST API), define a `glz::meta<Struct>`
specialization **in the same header as the struct**:

```cpp
// entities/Product.h
struct Product {
    int64_t id = 0;
    std::string product_name;
    int32_t unit_price = 0;
};

template<>
struct glz::meta<Product> {           // ← in the struct header
    using T = Product;
    static constexpr auto value = glz::object(
        "id", &T::id,
        "productName", &T::product_name,   // camelCase on the wire
        "unitPrice", &T::unit_price);
};
```

### How the generator avoids an ODR clash

The generator **scans the source header for an existing `glz::meta<Struct>`
specialization** (matching the bare class name). If it finds one, it emits
**none** — yours wins. If it finds none, it emits a default mapping using the
member names.

The consequence — and the footgun:

- ✅ **Define `glz::meta<Struct>` in the same header as the struct.** Detected,
  generator stays silent, no duplicate.
- ❌ **Define it in a *separate* header.** The generator can't see it, emits its
  own, and you get **two definitions of `glz::meta<Struct>` → ODR violation**
  (duplicate-symbol / redefinition errors).

Since the struct header is framework-agnostic, every consumer that includes it —
the API and any BEVE client — shares the same naming contract. For BEVE
interop, both sides need matching `glz::meta` keys.

## CMake integration

A reusable module drives generation as a build step. See
[`cmake/RelaisGenerateWrappers.cmake`](../cmake/RelaisGenerateWrappers.cmake).

### FetchContent (most common)

`include(RelaisGenerateWrappers)` only works once the module directory is on
`CMAKE_MODULE_PATH`. With FetchContent that does **not** happen automatically —
add it explicitly:

```cmake
FetchContent_MakeAvailable(relais)

# Required: put relais's cmake/ on the module path so include() can find it
list(APPEND CMAKE_MODULE_PATH "${relais_SOURCE_DIR}/cmake")
include(RelaisGenerateWrappers)

relais_generate_wrappers(
    SOURCES   ${CMAKE_CURRENT_SOURCE_DIR}/src/entities/
    OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/entities/generated/
)
add_dependencies(my_app relais_generate_wrappers)
```

`relais_SOURCE_DIR` is set by `FetchContent_MakeAvailable(relais)` (lowercase
name). The module resolves the Python script in-tree or post-install
automatically.

### Installed relais

When relais was `install()`-ed, its module directory is already on
`CMAKE_MODULE_PATH` via the package config — `include(RelaisGenerateWrappers)`
works without the extra `list(APPEND ...)`.

## What the Mapping contains

For each entity the generator produces, inside `entity::generated`:

- **`{Class}Mapping`** — `table_name`, `primary_key_column(s)`, a `Col` index
  enum, a `SQL` struct (`select_by_pk`, `insert`, `update`, `delete_by_pk`,
  `select_by_pk_batch`), `fromRow`/`toInsertParams`/`key`, a `RowView` +
  `rowToJson`/`rowToBeve` for zero-copy serialization, `TraitsType` (the `Field`
  enum + `FieldInfo` for `patch`), and `dynamicSize` for memory accounting.
- **`{Class}Entity`** — the `Entity<Struct, Mapping>` alias (public API type).
- **Partition-key entities** — `SQL::delete_with_partition` +
  `makePartitionHintParams()`.
- **List entities** — an embedded `ListDescriptor` (auto-detected by ListMixin)
  and a `{Class}ListWrapper` alias.
</content>