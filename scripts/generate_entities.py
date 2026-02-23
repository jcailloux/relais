#!/usr/bin/env python3
"""
Struct Entity Wrapper Generator

Scans C++ header files for @relais annotations, parses struct data members,
and generates:
  - Standalone Mapping structs with fromRow/toInsertParams/TraitsType/FieldInfo
  - EntityWrapper<Struct, Mapping> type aliases (public API)
  - ListWrapper and ListDescriptor for list entities

Uses jcailloux::relais::io for database access.

Usage:
    python generate_entities.py --sources dir/ --output-dir generated/
    python generate_entities.py --sources file1.h file2.h --output-dir generated/
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class DataMember:
    """A parsed C++ struct data member."""
    name: str
    cpp_type: str
    default: str = ""
    db_column: str = ""
    tags: set[str] = field(default_factory=set)
    enum_pairs: list[tuple[str, str]] = field(default_factory=list)
    filter_configs: list[FilterConfig] = field(default_factory=list)
    sort_configs: list[SortConfig] = field(default_factory=list)

    @property
    def col_name(self) -> str:
        """DB column name (falls back to C++ field name)."""
        return self.db_column or self.name

    @property
    def is_optional(self) -> bool:
        return self.cpp_type.startswith("std::optional<")

    @property
    def inner_type(self) -> str:
        """For std::optional<T>, return T. Otherwise return the type itself."""
        if self.is_optional:
            return self.cpp_type[len("std::optional<"):-1]
        return self.cpp_type

    @property
    def is_string(self) -> bool:
        return self.cpp_type == "std::string" or self.inner_type == "std::string"

    @property
    def is_raw_json(self) -> bool:
        return "raw_json" in self.cpp_type


@dataclass
class EnumMapping:
    """Enum field mapping: DB string <-> C++ enum value."""
    field_name: str
    pairs: list[tuple[str, str]]  # [(db_value, enum_value), ...]
    cpp_type: str = ""  # C++ type name (e.g., "Priority")


@dataclass
class FilterConfig:
    param: str      # HTTP query parameter name
    field: str      # Struct field name (→ &Entity::field)
    op: str = "eq"  # Comparison operator: eq, ne, gt, ge, lt, le


@dataclass
class SortConfig:
    param: str            # HTTP query parameter name
    field: str            # Struct field name
    direction: str = "desc"  # asc or desc


@dataclass
class EntityAnnotation:
    """Parsed @relais annotations for an entity."""
    table: str = ""       # PostgreSQL table name
    model: str = ""       # Parsed but ignored (table= is used instead)
    primary_keys: list[str] = field(default_factory=list)  # multiple @relais primary_key fields
    partition_keys: list[str] = field(default_factory=list)
    db_managed: list[str] = field(default_factory=list)
    timestamps: list[str] = field(default_factory=list)
    raw_json: list[str] = field(default_factory=list)
    json_fields: list[str] = field(default_factory=list)
    enums: list[EnumMapping] = field(default_factory=list)
    read_only: bool = False
    # List annotations
    filters: list[FilterConfig] = field(default_factory=list)
    sorts: list[SortConfig] = field(default_factory=list)
    limits: list[int] = field(default_factory=list)
    entity_fqn: str = ""  # Fully qualified entity class name for Descriptor

    @property
    def primary_key(self) -> str:
        """First (or only) primary key field name. Falls back to 'id'."""
        return self.primary_keys[0] if self.primary_keys else "id"

    @property
    def is_composite(self) -> bool:
        """True if entity has a composite primary key (multiple @relais primary_key fields)."""
        return len(self.primary_keys) > 1

    @property
    def has_list(self) -> bool:
        return bool(self.filters or self.sorts or self.limits)


@dataclass
class ParsedEntity:
    """A fully parsed entity from a header file."""
    class_name: str
    namespace: str
    annotation: EntityAnnotation
    members: list[DataMember]
    source_file: Path


# =============================================================================
# Parser
# =============================================================================

class StructParser:
    """Parse C++ headers for @relais annotations and struct data members."""

    # Regex: @relais or @relais_list annotations in comments
    ANNOTATION_RE = re.compile(r'//\s*@relais(?:_list)?\s+(.*)')
    # Regex: class/struct Name [: public ...] {
    CLASS_RE = re.compile(r'(?:class|struct)\s+(\w+)')
    # Regex: data member — type name [= default]; [// @relais ...]
    MEMBER_RE = re.compile(
        r'^\s+([\w:<>,\s]+?)\s+(\w+)\s*(?:=\s*([^;]+))?\s*;(.*)',
        re.MULTILINE
    )
    # Regex: namespace ... {
    NAMESPACE_RE = re.compile(r'namespace\s+([\w:]+)\s*\{')

    def parse_file(self, filepath: Path) -> list[ParsedEntity]:
        """Parse a header file and return all annotated entities."""
        content = filepath.read_text()
        entities = []

        # Find all @relais annotation blocks followed by a class/struct declaration
        lines = content.split('\n')
        i = 0
        while i < len(lines):
            # Look for @relais annotation block
            annotations = []
            while i < len(lines) and self.ANNOTATION_RE.match(lines[i].strip()):
                m = self.ANNOTATION_RE.match(lines[i].strip())
                annotations.append((
                    '@relais_list' in lines[i],
                    m.group(1).strip()
                ))
                i += 1

            if not annotations:
                i += 1
                continue

            # Find class/struct declaration (may follow immediately or after blank lines)
            while i < len(lines) and not self.CLASS_RE.search(lines[i]):
                i += 1
                if i >= len(lines):
                    break

            if i >= len(lines):
                break

            class_match = self.CLASS_RE.search(lines[i])
            if not class_match:
                i += 1
                continue

            class_name = class_match.group(1)

            # Parse annotation key=value pairs
            annot = self._parse_annotations(annotations)

            # Find namespace
            ns = self._find_namespace(content, lines[i])

            # Set entity_fqn for Descriptor if not specified
            if not annot.entity_fqn and ns:
                annot.entity_fqn = f"{ns}::{class_name}"
            elif not annot.entity_fqn:
                annot.entity_fqn = class_name

            # Parse data members from the class body (with inline annotations)
            members = self._parse_members(lines, i)

            # Apply inline member annotations to the EntityAnnotation
            self._apply_member_annotations(annot, members)

            entities.append(ParsedEntity(
                class_name=class_name,
                namespace=ns,
                annotation=annot,
                members=members,
                source_file=filepath,
            ))

            i += 1

        return entities

    def _parse_annotations(self, annotations: list[tuple[bool, str]]) -> EntityAnnotation:
        """Parse annotation key=value pairs."""
        annot = EntityAnnotation()

        for is_list, text in annotations:
            # Split by whitespace, preserving key=value pairs
            tokens = self._tokenize_annotation(text)

            if is_list:
                for token in tokens:
                    if token.startswith("limits="):
                        annot.limits = [int(x) for x in token[7:].split(",")]
                    elif token.startswith("entity="):
                        annot.entity_fqn = token[7:]
            else:
                for token in tokens:
                    if token.startswith("table="):
                        annot.table = token[6:]
                    elif token.startswith("model="):
                        annot.model = token[6:]
                    elif token == "read_only" or token == "read_only=true":
                        annot.read_only = True

        return annot

    def _tokenize_annotation(self, text: str) -> list[str]:
        """Split annotation text by spaces, but keep enum=... together."""
        tokens = []
        current = ""
        for ch in text:
            if ch == ' ' and current and '=' not in current:
                if current:
                    tokens.append(current)
                current = ""
            elif ch == ' ' and current:
                tokens.append(current)
                current = ""
            else:
                current += ch
        if current:
            tokens.append(current)
        return tokens

    def _parse_enum_mapping(self, text: str) -> EnumMapping:
        """Parse enum=field:val1:Enum1,val2:Enum2"""
        parts = text.split(":")
        field_name = parts[0]
        rest = ":".join(parts[1:])
        pairs = []
        for pair_str in rest.split(","):
            pair_parts = pair_str.split(":")
            if len(pair_parts) == 2:
                pairs.append((pair_parts[0], pair_parts[1]))
        return EnumMapping(field_name, pairs)

    def _apply_member_annotations(self, annot: EntityAnnotation,
                                    members: list[DataMember]):
        """Populate EntityAnnotation fields from inline member @relais tags."""
        for m in members:
            if 'primary_key' in m.tags:
                annot.primary_keys.append(m.name)
            if 'partition_key' in m.tags:
                annot.partition_keys.append(m.name)
            if 'db_managed' in m.tags:
                annot.db_managed.append(m.name)
            if 'timestamp' in m.tags:
                annot.timestamps.append(m.name)
            if 'raw_json' in m.tags:
                annot.raw_json.append(m.name)
            if 'json_field' in m.tags:
                annot.json_fields.append(m.name)
            if m.enum_pairs:
                annot.enums.append(EnumMapping(m.name, m.enum_pairs, m.inner_type))
            elif 'auto_enum' in m.tags:
                annot.enums.append(EnumMapping(m.name, [], m.inner_type))
            for fc in m.filter_configs:
                annot.filters.append(fc)
            for sc in m.sort_configs:
                annot.sorts.append(sc)

    @staticmethod
    def _parse_filterable_tag(field_name: str, tag: str) -> FilterConfig:
        """Parse filterable[:param[:op]] from an inline annotation.

        Examples:
            filterable              -> EQ on field_name
            filterable:custom_name  -> EQ with custom HTTP param
            filterable:ge           -> GE operator (known op) on field_name
            filterable:date_from:ge -> custom param + GE operator
        """
        KNOWN_OPS = {"eq", "ne", "gt", "ge", "lt", "le", "gte", "lte"}
        parts = tag.split(":")
        if len(parts) == 1:
            return FilterConfig(param=field_name, field=field_name)
        elif len(parts) == 2:
            val = parts[1]
            if val in KNOWN_OPS:
                op = val.replace("gte", "ge").replace("lte", "le")
                return FilterConfig(param=field_name, field=field_name, op=op)
            return FilterConfig(param=val, field=field_name)
        else:
            op = parts[2].replace("gte", "ge").replace("lte", "le")
            return FilterConfig(param=parts[1], field=field_name, op=op)

    @staticmethod
    def _parse_sortable_tag(field_name: str, tag: str) -> SortConfig:
        """Parse sortable[:direction] or sortable[:param:direction].

        Examples:
            sortable            -> DESC (default)
            sortable:asc        -> ASC
            sortable:desc       -> DESC
            sortable:name:asc   -> custom param + ASC
        """
        DIRECTIONS = {"asc", "desc"}
        parts = tag.split(":")
        if len(parts) == 1:
            return SortConfig(param=field_name, field=field_name)
        elif len(parts) == 2:
            val = parts[1]
            if val in DIRECTIONS:
                return SortConfig(param=field_name, field=field_name,
                                  direction=val)
            return SortConfig(param=val, field=field_name)
        else:
            return SortConfig(param=parts[1], field=field_name,
                              direction=parts[2])

    def _find_namespace(self, content: str, class_line: str) -> str:
        """Find the namespace enclosing the class declaration."""
        pos = content.find(class_line)
        if pos < 0:
            return ""
        before = content[:pos]
        matches = list(self.NAMESPACE_RE.finditer(before))
        return matches[-1].group(1) if matches else ""

    def _parse_members(self, lines: list[str], class_line_idx: int) -> list[DataMember]:
        """Parse data members from a class/struct body."""
        members = []
        brace_depth = 0
        in_public = False
        is_struct = 'struct ' in lines[class_line_idx]

        for i in range(class_line_idx, len(lines)):
            line = lines[i]
            if '{' in line:
                brace_depth += line.count('{') - line.count('}')
                if i == class_line_idx and is_struct:
                    in_public = True
                continue

            if brace_depth == 0 and i > class_line_idx:
                break

            brace_depth += line.count('{') - line.count('}')

            stripped = line.strip()
            if stripped == "public:":
                in_public = True
                continue
            if stripped in ("private:", "protected:"):
                in_public = False
                continue

            if not in_public:
                continue

            if "[[nodiscard]]" in stripped or "(" in stripped:
                continue
            if stripped.startswith("//") or stripped.startswith("using "):
                continue
            if not stripped or stripped == "};":
                continue

            m = self.MEMBER_RE.match(line)
            if m:
                cpp_type = m.group(1).strip()
                name = m.group(2)
                default = m.group(3).strip() if m.group(3) else ""
                tags = set()
                db_column = ""
                enum_pairs = []
                flt_configs = []
                srt_configs = []
                trailing = m.group(4).strip() if m.group(4) else ""
                if '@relais' in trailing:
                    idx = trailing.index('@relais') + len('@relais')
                    tag_text = trailing[idx:].strip()
                    for tag in tag_text.split():
                        if tag.startswith('enum='):
                            for pair in tag[5:].split(','):
                                parts = pair.split(':')
                                if len(parts) == 2:
                                    enum_pairs.append((parts[0], parts[1]))
                        elif tag == 'enum':
                            tags.add('auto_enum')
                        elif tag.startswith('column='):
                            db_column = tag[7:]
                        elif tag.startswith('filterable'):
                            flt_configs.append(
                                self._parse_filterable_tag(name, tag))
                        elif tag.startswith('sortable'):
                            srt_configs.append(
                                self._parse_sortable_tag(name, tag))
                        else:
                            tags.add(tag)
                members.append(DataMember(
                    name, cpp_type, default, db_column, tags, enum_pairs,
                    flt_configs, srt_configs))

        return members


# =============================================================================
# Code Generator
# =============================================================================

class MappingGenerator:
    """Generate Mapping structs + EntityWrapper aliases from parsed entities.

    Generates SQL-direct code using jcailloux::relais::io.
    """

    def __init__(self):
        pass

    def _resolve_table_name(self, entity: ParsedEntity) -> str:
        """Resolve PostgreSQL table name from annotations."""
        a = entity.annotation
        if a.table:
            return a.table
        # Fallback: derive from class name (snake_case)
        name = entity.class_name
        # CamelCase -> snake_case
        s = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        print(f"  Warning: No table= annotation for {entity.class_name}, "
              f"using derived name '{s}'", file=sys.stderr)
        return s

    def generate(self, entity: ParsedEntity, output_file: Path) -> str:
        """Generate the complete wrapper header."""
        a = entity.annotation
        mapping_name = f"{entity.class_name}Mapping"
        wrapper_name = f"{entity.class_name}Wrapper"

        # Determine updateable fields (exclude PK, db_managed, json_fields)
        updateable = self._get_updateable_fields(entity)

        # Resolve auto-enum pairs from glz::meta before code generation
        source_content = entity.source_file.read_text()
        self._resolve_auto_enums(entity, source_content)

        lines = [
            "// GENERATED AUTOMATICALLY - DO NOT MODIFY",
            f"// Source: {entity.source_file.name}",
            "",
            "#pragma once",
            "",
        ]

        # Includes
        lines.extend(self._generate_includes(entity, output_file))
        lines.append("")
        lines.append("namespace entity::generated {")
        lines.append("")

        # Mapping struct
        lines.extend(self._generate_mapping_struct(entity, mapping_name, updateable))
        lines.append("")

        # FieldInfo specializations (at namespace scope)
        lines.extend(self._generate_field_info(entity, mapping_name, updateable))

        # === Wrapper alias ===
        struct_fqn = f"{entity.namespace}::{entity.class_name}" if entity.namespace else entity.class_name
        lines.append("// ============================================================================")
        lines.append(f"// {wrapper_name} — public API type")
        lines.append("// ============================================================================")
        lines.append("")
        lines.append(f"using {wrapper_name} = jcailloux::relais::wrapper::EntityWrapper<")
        lines.append(f"    {struct_fqn}, {mapping_name}>;")

        # === List wrapper (if list annotations present) ===
        if a.has_list:
            list_wrapper_name = f"{entity.class_name}ListWrapper"
            lines.append("")
            lines.append(f"using {list_wrapper_name} = jcailloux::relais::wrapper::ListWrapper<{wrapper_name}>;")

        lines.append("")
        lines.append("}  // namespace entity::generated")

        # === glz::meta for struct, enums, and RowView (outside namespace) ===
        struct_meta = self._generate_struct_meta(entity, source_content)
        enum_metas = self._generate_enum_metas(entity, source_content)
        row_view_meta = self._generate_row_view_meta(entity, mapping_name)
        if struct_meta:
            lines.append("")
            lines.extend(struct_meta)
        if enum_metas:
            lines.append("")
            lines.extend(enum_metas)
        if row_view_meta:
            lines.append("")
            lines.extend(row_view_meta)

        lines.append("")

        return "\n".join(lines)

    # =========================================================================
    # Includes
    # =========================================================================

    def _generate_includes(self, entity: ParsedEntity, output_file: Path) -> list[str]:
        a = entity.annotation
        lines = [
            "#include <cstdint>",
            "#include <optional>",
            "#include <string>",
            "#include <string_view>",
        ]

        # Composite key needs <tuple> and <array>
        if a.is_composite:
            lines.append("#include <array>")
            lines.append("#include <tuple>")

        # Glaze for json_fields deserialization
        if a.json_fields:
            lines.append("#include <glaze/glaze.hpp>")

        # vector<char> for bytea fields
        has_vector_char = any(
            m.cpp_type == "std::vector<char>" or m.inner_type == "std::vector<char>"
            for m in entity.members
        )
        if has_vector_char:
            lines.append("#include <vector>")

        # EntityWrapper (always needed)
        lines.append("#include <jcailloux/relais/wrapper/EntityWrapper.h>")

        # Struct header (relative path from generated file to source)
        struct_include = self._find_struct_include(entity, output_file)
        lines.append(f'#include "{struct_include}"')

        # List includes
        if a.has_list:
            lines.append("#include <jcailloux/relais/wrapper/ListWrapper.h>")
            lines.append("#include <jcailloux/relais/list/decl/FilterDescriptor.h>")
            lines.append("#include <jcailloux/relais/list/decl/SortDescriptor.h>")

        return lines

    @staticmethod
    def _find_struct_include(entity: ParsedEntity, output_file: Path) -> str:
        """Return include path for the struct header, relative to the generated file."""
        return os.path.relpath(entity.source_file, output_file.parent)

    # =========================================================================
    # Mapping struct
    # =========================================================================

    def _generate_mapping_struct(self, entity: ParsedEntity, mapping_name: str,
                                   updateable: list[DataMember]) -> list[str]:
        a = entity.annotation
        table_name = self._resolve_table_name(entity)

        lines = [
            f"struct {mapping_name} {{",
            f"    static constexpr bool read_only = {'true' if a.read_only else 'false'};",
            f'    static constexpr const char* table_name = "{table_name}";',
        ]

        if a.is_composite:
            pk_cols_str = ", ".join(f'"{self._col(entity, pk)}"' for pk in a.primary_keys)
            lines.append(f"    static constexpr std::array<const char*, {len(a.primary_keys)}> primary_key_columns = {{{pk_cols_str}}};")
        else:
            lines.append(f'    static constexpr const char* primary_key_column = "{self._col(entity, a.primary_key)}";')

        lines.append("")

        # Col enum — column indices for O(1) access in fromRow
        col_entries = ", ".join(
            f"{m.name} = {i}" for i, m in enumerate(entity.members))
        lines.append(f"    enum Col : uint8_t {{ {col_entries} }};")
        lines.append("")

        # SQL struct — pre-built SQL strings
        lines.extend(self._generate_sql_struct(entity, table_name))
        lines.append("")

        # Nested TraitsType
        lines.extend(self._generate_traits_type(entity, updateable))
        lines.append("")

        # key
        lines.append("    template<typename Entity>")
        lines.append("    static auto key(const Entity& e) noexcept {")
        if a.is_composite:
            pk_args = ", ".join(f"e.{pk}" for pk in a.primary_keys)
            lines.append(f"        return std::make_tuple({pk_args});")
        else:
            lines.append(f"        return e.{a.primary_key};")
        lines.append("    }")
        lines.append("")

        # makePartitionHintParams (for partition pruning — distinct from composite key)
        if a.partition_keys:
            args = [f"e.{a.primary_key}"] + [f"e.{pk}" for pk in a.partition_keys]
            args_str = ",\n            ".join(args)
            lines.append("    template<typename Entity>")
            lines.append("    static jcailloux::relais::io::PgParams makePartitionHintParams(const Entity& e) {")
            lines.append(f"        return jcailloux::relais::io::PgParams::make(")
            lines.append(f"            {args_str}")
            lines.append(f"        );")
            lines.append("    }")
            lines.append("")

        # fromRow
        lines.extend(self._generate_from_row(entity))

        # toInsertParams (skip for read-only)
        if not a.read_only:
            lines.append("")
            lines.extend(self._generate_to_insert_params(entity))

        # toUpdateParams (skip for read-only; only generated for composite keys)
        if not a.read_only and a.is_composite:
            lines.append("")
            lines.extend(self._generate_to_update_params(entity))

        # dynamicSize (heap memory for strings/vectors)
        lines.append("")
        lines.extend(self._generate_dynamic_size(entity))

        # Glaze metadata template
        lines.append("")
        lines.extend(self._generate_glaze_value(entity))

        # RowView + rowToJson + rowToBeve (direct serialization from PgResult)
        lines.append("")
        lines.extend(self._generate_row_view(entity))

        # Embedded ListDescriptor (if list annotations present)
        if a.has_list:
            lines.append("")
            lines.extend(self._generate_embedded_descriptor(entity))

        lines.append("};")
        return lines

    # =========================================================================
    # SQL struct (nested inside Mapping)
    # =========================================================================

    def _generate_sql_struct(self, entity: ParsedEntity, table_name: str) -> list[str]:
        a = entity.annotation
        all_cols = [m.col_name for m in entity.members]
        all_cols_str = ", ".join(all_cols)

        # INSERT: skip db_managed fields
        insert_cols = [m.col_name for m in entity.members if m.name not in a.db_managed]
        insert_cols_str = ", ".join(insert_cols)
        insert_params = ", ".join(f"${i+1}" for i in range(len(insert_cols)))

        # PK columns for WHERE clauses
        pk_cols = [self._col(entity, pk) for pk in a.primary_keys] if a.primary_keys else [self._col(entity, "id")]
        pk_count = len(pk_cols)

        # WHERE clause for PK: pk1=$1 AND pk2=$2 ...
        pk_where = " AND ".join(f"{col} = ${i+1}" for i, col in enumerate(pk_cols))

        # UPDATE: skip all PKs and db_managed fields
        pk_set = set(a.primary_keys) if a.primary_keys else {"id"}
        update_cols = [m.col_name for m in entity.members
                       if m.name not in pk_set and m.name not in a.db_managed]
        # SET params start after PK params: $pk_count+1, $pk_count+2, ...
        update_set = ", ".join(
            f"{col}=${i + pk_count + 1}" for i, col in enumerate(update_cols))

        lines = [
            "    struct SQL {",
            f'        static constexpr const char* returning_columns =',
            f'            "{all_cols_str}";',
            f"        static constexpr const char* select_by_pk =",
            f'            "SELECT {all_cols_str} '
            f'FROM {table_name} WHERE {pk_where}";',
            f"        static constexpr const char* insert =",
            f'            "INSERT INTO {table_name} ({insert_cols_str}) '
            f'VALUES ({insert_params}) RETURNING {all_cols_str}";',
        ]

        if not a.read_only:
            lines.append(f"        static constexpr const char* update =")
            lines.append(
                f'            "UPDATE {table_name} SET {update_set} '
                f'WHERE {pk_where}";')

        # SELECT ... WHERE pk = ANY($1) for batch reads
        if pk_count == 1:
            # Simple key: WHERE pk_col = ANY($1)
            batch_where = f"{pk_cols[0]} = ANY($1)"
        else:
            # Composite key: WHERE (k1, k2) IN (SELECT unnest($1::type[]), unnest($2::type[]))
            unnest_parts = ", ".join(f"unnest(${i+1})" for i in range(pk_count))
            key_tuple = ", ".join(pk_cols)
            batch_where = f"({key_tuple}) IN (SELECT {unnest_parts})"
        lines.append(f"        static constexpr const char* select_by_pk_batch =")
        lines.append(
            f'            "SELECT {all_cols_str} '
            f'FROM {table_name} WHERE {batch_where}";')

        lines.append(f"        static constexpr const char* delete_by_pk =")
        lines.append(
            f'            "DELETE FROM {table_name} WHERE {pk_where}";')

        # Partition-pruned DELETE (when partition_keys present, distinct from composite PK)
        if a.partition_keys:
            hint_where_parts = [f"{col} = ${i+1}" for i, col in enumerate(pk_cols)]
            for i, pk in enumerate(a.partition_keys):
                pk_db_col = self._col(entity, pk)
                hint_where_parts.append(f"{pk_db_col} = ${pk_count + i + 1}")
            hint_where_clause = " AND ".join(hint_where_parts)
            lines.append(f"        static constexpr const char* delete_with_partition =")
            lines.append(
                f'            "DELETE FROM {table_name} WHERE {hint_where_clause}";')

        lines.append("    };")

        return lines

    # =========================================================================
    # TraitsType (nested inside Mapping)
    # =========================================================================

    def _generate_traits_type(self, entity: ParsedEntity,
                               updateable: list[DataMember]) -> list[str]:
        a = entity.annotation
        lines = [
            "    struct TraitsType {",
        ]

        if updateable and not a.read_only:
            lines.append("        enum class Field : uint8_t {")
            for i, m in enumerate(updateable):
                comma = "," if i < len(updateable) - 1 else ""
                lines.append(f"            {m.name}{comma}")
            lines.append("        };")
            lines.append("")
            lines.append("        template<Field> struct FieldInfo;")

        lines.append("    };")
        return lines

    # =========================================================================
    # fromRow (template method inside Mapping struct)
    # =========================================================================

    def _generate_from_row(self, entity: ParsedEntity) -> list[str]:
        a = entity.annotation
        lines = [
            "    template<typename Entity>",
            "    static std::optional<Entity> fromRow(const jcailloux::relais::io::PgResult::Row& row) {",
            "        Entity e;",
        ]

        for m in entity.members:
            enum_mapping = self._find_enum_mapping(a, m.name)

            if m.name in a.json_fields:
                # JSON field: deserialize from string column
                if m.is_optional:
                    lines.append(f"        if (!row.isNull(Col::{m.name})) {{")
                    lines.append(f"            auto json_str = row.get<std::string>(Col::{m.name});")
                    lines.append(f"            if (!json_str.empty()) {{")
                    lines.append(f"                typename std::remove_reference_t<decltype(*e.{m.name})> tmp;")
                    lines.append(f"                if (!glz::read_json(tmp, json_str))")
                    lines.append(f"                    e.{m.name} = std::move(tmp);")
                    lines.append(f"            }}")
                    lines.append(f"        }}")
                else:
                    lines.append(f"        glz::read_json(e.{m.name}, row.get<std::string>(Col::{m.name}));")
            elif m.is_raw_json:
                # Raw JSON: store as glz::raw_json
                lines.append(f"        e.{m.name}.str = row.get<std::string>(Col::{m.name});")
            elif enum_mapping:
                # Enum: convert string -> enum
                enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                if m.is_optional:
                    lines.append(f"        if (!row.isNull(Col::{m.name})) {{")
                    lines.append(f"            auto s = row.get<std::string>(Col::{m.name});")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") e.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
                else:
                    lines.append(f"        {{")
                    lines.append(f"            auto s = row.get<std::string>(Col::{m.name});")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") e.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
            elif m.is_optional:
                # Nullable: use getOpt<T>
                lines.append(f"        e.{m.name} = row.getOpt<{m.inner_type}>(Col::{m.name});")
            else:
                # Simple field: direct get<T>
                lines.append(f"        e.{m.name} = row.get<{m.cpp_type}>(Col::{m.name});")

        lines.append("        return e;")
        lines.append("    }")
        return lines

    # =========================================================================
    # toInsertParams (template method inside Mapping struct)
    # =========================================================================

    def _generate_to_insert_params(self, entity: ParsedEntity) -> list[str]:
        a = entity.annotation

        # Determine which fields go into INSERT (skip db_managed)
        insert_members = [m for m in entity.members if m.name not in a.db_managed]

        # Check if we have any special types that prevent simple PgParams::make
        has_special = any(
            m.name in a.json_fields or m.is_raw_json or
            self._find_enum_mapping(a, m.name)
            for m in insert_members
        )

        lines = [
            "    template<typename Entity>",
            "    static jcailloux::relais::io::PgParams toInsertParams(const Entity& e) {",
        ]

        # Skip comment for db_managed fields
        for m in entity.members:
            if m.name in a.db_managed:
                lines.append(f"        // {m.name}: skipped (db_managed)")
                break

        if not has_special:
            # Simple case: use PgParams::make
            args = []
            for m in insert_members:
                args.append(f"            e.{m.name}")
            lines.append("        return jcailloux::relais::io::PgParams::make(")
            lines.append(",\n".join(args))
            lines.append("        );")
        else:
            # Complex case: build params manually for enum/json conversion
            lines.append("        jcailloux::relais::io::PgParams p;")
            for m in insert_members:
                enum_mapping = self._find_enum_mapping(a, m.name)
                if m.name in a.json_fields:
                    if m.is_optional:
                        lines.append(f"        if (e.{m.name}) {{")
                        lines.append(f"            std::string json;")
                        lines.append(f"            glz::write_json(*e.{m.name}, json);")
                        lines.append(f"            p.push(json);")
                        lines.append(f"        }} else {{")
                        lines.append(f"            p.pushNull();")
                        lines.append(f"        }}")
                    else:
                        lines.append(f"        {{ std::string json; glz::write_json(e.{m.name}, json); p.push(json); }}")
                elif m.is_raw_json:
                    lines.append(f"        p.push(e.{m.name}.str);")
                elif enum_mapping:
                    enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                    if m.is_optional:
                        lines.append(f"        if (e.{m.name}.has_value()) {{")
                        lines.append(f"            switch (*e.{m.name}) {{")
                        for db_val, enum_val in enum_mapping.pairs:
                            lines.append(f'                case {enum_fqn}::{enum_val}: p.push("{db_val}"); break;')
                        lines.append(f"            }}")
                        lines.append(f"        }} else {{")
                        lines.append(f"            p.pushNull();")
                        lines.append(f"        }}")
                    else:
                        lines.append(f"        switch (e.{m.name}) {{")
                        for db_val, enum_val in enum_mapping.pairs:
                            lines.append(f'            case {enum_fqn}::{enum_val}: p.push("{db_val}"); break;')
                        lines.append(f"        }}")
                else:
                    lines.append(f"        p.push(e.{m.name});")
            lines.append("        return p;")

        lines.append("    }")
        return lines

    # =========================================================================
    # toUpdateParams (for composite keys: only non-PK, non-db_managed fields)
    # =========================================================================

    def _generate_to_update_params(self, entity: ParsedEntity) -> list[str]:
        """Generate toUpdateParams for composite key entities.

        Unlike toInsertParams (which includes PK fields), this returns only
        the SET fields used in UPDATE statements. The caller (BaseRepo) prepends
        the key params separately.
        """
        a = entity.annotation
        pk_set = set(a.primary_keys)

        # Fields for SET: not PK, not db_managed
        update_members = [
            m for m in entity.members
            if m.name not in pk_set and m.name not in a.db_managed
        ]

        has_special = any(
            m.name in a.json_fields or m.is_raw_json or
            self._find_enum_mapping(a, m.name)
            for m in update_members
        )

        lines = [
            "    template<typename Entity>",
            "    static jcailloux::relais::io::PgParams toUpdateParams(const Entity& e) {",
        ]

        if not has_special:
            args = [f"            e.{m.name}" for m in update_members]
            lines.append("        return jcailloux::relais::io::PgParams::make(")
            lines.append(",\n".join(args))
            lines.append("        );")
        else:
            lines.append("        jcailloux::relais::io::PgParams p;")
            for m in update_members:
                enum_mapping = self._find_enum_mapping(a, m.name)
                if m.name in a.json_fields:
                    if m.is_optional:
                        lines.append(f"        if (e.{m.name}) {{")
                        lines.append(f"            std::string json;")
                        lines.append(f"            glz::write_json(*e.{m.name}, json);")
                        lines.append(f"            p.push(json);")
                        lines.append(f"        }} else {{")
                        lines.append(f"            p.pushNull();")
                        lines.append(f"        }}")
                    else:
                        lines.append(f"        {{ std::string json; glz::write_json(e.{m.name}, json); p.push(json); }}")
                elif m.is_raw_json:
                    lines.append(f"        p.push(e.{m.name}.str);")
                elif enum_mapping:
                    enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                    if m.is_optional:
                        lines.append(f"        if (e.{m.name}.has_value()) {{")
                        lines.append(f"            switch (*e.{m.name}) {{")
                        for db_val, enum_val in enum_mapping.pairs:
                            lines.append(f'                case {enum_fqn}::{enum_val}: p.push("{db_val}"); break;')
                        lines.append(f"            }}")
                        lines.append(f"        }} else {{")
                        lines.append(f"            p.pushNull();")
                        lines.append(f"        }}")
                    else:
                        lines.append(f"        switch (e.{m.name}) {{")
                        for db_val, enum_val in enum_mapping.pairs:
                            lines.append(f'            case {enum_fqn}::{enum_val}: p.push("{db_val}"); break;')
                        lines.append(f"        }}")
                else:
                    lines.append(f"        p.push(e.{m.name});")
            lines.append("        return p;")

        lines.append("    }")
        return lines

    # =========================================================================
    # Glaze metadata (variable template inside Mapping)
    # =========================================================================

    def _generate_dynamic_size(self, entity: ParsedEntity) -> list[str]:
        """Generate dynamicSize() — heap memory used by dynamic fields (strings, vectors, raw_json).

        Uses heapCapacity() for std::string to exclude SSO buffer (already in sizeof).
        """
        a = entity.annotation
        hc = "jcailloux::relais::wrapper::heapCapacity"
        # Collect fields with dynamic heap allocations
        dynamic_fields = []
        for m in entity.members:
            if m.cpp_type == "std::string":
                dynamic_fields.append(f"{hc}(s.{m.name})")
            elif m.cpp_type == "std::optional<std::string>":
                dynamic_fields.append(f"(s.{m.name} ? {hc}(*s.{m.name}) : 0)")
            elif m.cpp_type == "std::vector<char>":
                dynamic_fields.append(f"s.{m.name}.capacity()")
            elif m.cpp_type == "std::optional<std::vector<char>>":
                dynamic_fields.append(f"(s.{m.name} ? s.{m.name}->capacity() : 0)")
            elif m.is_raw_json:
                dynamic_fields.append(f"{hc}(s.{m.name}.str)")

        if not dynamic_fields:
            return []

        struct_fqn = f"{entity.namespace}::{entity.class_name}" if entity.namespace else entity.class_name
        lines = [
            f"    static size_t dynamicSize(const {struct_fqn}& s) {{",
            f"        return {' + '.join(dynamic_fields)};",
            f"    }}",
        ]
        return lines

    def _generate_glaze_value(self, entity: ParsedEntity) -> list[str]:
        """Generate template<typename T> static constexpr auto glaze_value."""
        lines = [
            "    template<typename T>",
            "    static constexpr auto glaze_value = glz::object(",
        ]
        for i, m in enumerate(entity.members):
            comma = "," if i < len(entity.members) - 1 else ""
            lines.append(f'        "{m.name}", &T::{m.name}{comma}')
        lines.append("    );")
        return lines

    # =========================================================================
    # RowView — zero-copy view for direct serialization from PgResult
    # =========================================================================

    def _rowview_type(self, member: DataMember, annotation: EntityAnnotation,
                      entity: ParsedEntity) -> str:
        """Map entity type to RowView type (string_view for strings, raw_json_view for raw_json)."""
        if member.name in annotation.json_fields:
            return self._qualify_cpp_type(member.cpp_type, entity)
        if member.is_raw_json:
            return "glz::raw_json_view"
        if member.cpp_type == "std::string":
            return "std::string_view"
        if member.cpp_type == "std::optional<std::string>":
            return "std::optional<std::string_view>"
        return self._qualify_cpp_type(member.cpp_type, entity)

    _FUNDAMENTAL_TYPES = frozenset((
        "int8_t", "int16_t", "int32_t", "int64_t",
        "uint8_t", "uint16_t", "uint32_t", "uint64_t",
        "float", "double", "bool", "char",
    ))

    def _qualify_cpp_type(self, cpp_type: str, entity: ParsedEntity) -> str:
        """Qualify user-defined types with entity's namespace (for use in generated namespace)."""
        # Standard library templates: qualify inner type
        for prefix in ("std::optional<", "std::vector<"):
            if cpp_type.startswith(prefix):
                inner = cpp_type[len(prefix):-1]
                return f"{prefix}{self._qualify_cpp_type(inner, entity)}>"
        # Standard/Glaze types: no qualification
        if cpp_type.startswith("std::") or cpp_type.startswith("glz::"):
            return cpp_type
        if cpp_type in self._FUNDAMENTAL_TYPES:
            return cpp_type
        # Already qualified
        if "::" in cpp_type:
            return cpp_type
        # User-defined type: qualify with entity namespace
        if entity.namespace:
            return f"{entity.namespace}::{cpp_type}"
        return cpp_type

    def _generate_row_view(self, entity: ParsedEntity) -> list[str]:
        """Generate RowView struct + rowToJson + rowToBeve inside the Mapping."""
        a = entity.annotation
        lines = [
            "    // RowView — zero-copy view for direct serialization from PgResult",
            "    struct RowView {",
        ]

        _FUNDAMENTAL = frozenset((
            "int8_t", "int16_t", "int32_t", "int64_t",
            "uint8_t", "uint16_t", "uint32_t", "uint64_t",
            "float", "double", "bool",
        ))

        for m in entity.members:
            rv_type = self._rowview_type(m, a, entity)
            enum_mapping = self._find_enum_mapping(a, m.name)
            # Zero-init for fundamental types and non-optional enums
            if rv_type in _FUNDAMENTAL:
                default = " = {}"
            elif enum_mapping and not m.is_optional:
                default = " = {}"
            else:
                default = ""
            lines.append(f"        {rv_type} {m.name}{default};")

        lines.append("    };")
        lines.append("")

        # rowToJson
        lines.extend(self._generate_row_to_json(entity))
        lines.append("")

        # rowToBeve
        lines.extend(self._generate_row_to_beve(entity))

        return lines

    def _generate_rowview_populate(self, entity: ParsedEntity, var: str = "v") -> list[str]:
        """Generate code to populate a RowView from a PgResult::Row."""
        a = entity.annotation
        lines = []

        for m in entity.members:
            enum_mapping = self._find_enum_mapping(a, m.name)

            if m.name in a.json_fields:
                # JSON field: parse from string_view (avoids intermediate std::string alloc)
                if m.is_optional:
                    lines.append(f"        if (!row.isNull(Col::{m.name})) {{")
                    lines.append(f"            auto json_sv = row.get<std::string_view>(Col::{m.name});")
                    lines.append(f"            if (!json_sv.empty()) {{")
                    lines.append(f"                typename std::remove_reference_t<decltype(*{var}.{m.name})> tmp;")
                    lines.append(f"                if (!glz::read_json(tmp, json_sv))")
                    lines.append(f"                    {var}.{m.name} = std::move(tmp);")
                    lines.append(f"            }}")
                    lines.append(f"        }}")
                else:
                    lines.append(f"        (void)glz::read_json({var}.{m.name}, row.get<std::string_view>(Col::{m.name}));")
            elif m.is_raw_json:
                # Raw JSON: zero-copy string_view into raw_json_view
                lines.append(f"        {var}.{m.name}.str = row.get<std::string_view>(Col::{m.name});")
            elif enum_mapping:
                # Enum: compare string_view (zero alloc)
                enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                if m.is_optional:
                    lines.append(f"        if (!row.isNull(Col::{m.name})) {{")
                    lines.append(f"            auto s = row.get<std::string_view>(Col::{m.name});")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") {var}.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
                else:
                    lines.append(f"        {{")
                    lines.append(f"            auto s = row.get<std::string_view>(Col::{m.name});")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") {var}.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
            elif m.is_optional:
                # Optional: use getOpt with RowView-appropriate inner type
                inner = m.inner_type
                if inner == "std::string":
                    inner = "std::string_view"
                lines.append(f"        {var}.{m.name} = row.getOpt<{inner}>(Col::{m.name});")
            else:
                # Simple field: direct get with RowView type
                rv_type = self._rowview_type(m, a, entity)
                lines.append(f"        {var}.{m.name} = row.get<{rv_type}>(Col::{m.name});")

        return lines

    def _generate_row_to_json(self, entity: ParsedEntity) -> list[str]:
        """Generate rowToJson() static method inside the Mapping.
        Uses template<typename V = RowView> so that v's type is dependent, deferring
        glz::meta<RowView> lookup to instantiation time (after the specialization is visible)."""
        lines = [
            "    template<typename V = RowView>",
            "    static std::shared_ptr<const std::string> rowToJson(",
            "            const jcailloux::relais::io::PgResult::Row& row) {",
            "        V v;",
        ]
        lines.extend(self._generate_rowview_populate(entity))
        lines.extend([
            "        auto json = std::make_shared<std::string>();",
            "        json->reserve(256);",
            "        if (glz::write_json(v, *json))",
            "            return nullptr;",
            "        return json;",
            "    }",
        ])
        return lines

    def _generate_row_to_beve(self, entity: ParsedEntity) -> list[str]:
        """Generate rowToBeve() static method inside the Mapping.
        Uses template<typename V = RowView> so that v's type is dependent, deferring
        glz::meta<RowView> lookup to instantiation time (after the specialization is visible)."""
        lines = [
            "    template<typename V = RowView>",
            "    static std::shared_ptr<const std::vector<uint8_t>> rowToBeve(",
            "            const jcailloux::relais::io::PgResult::Row& row) {",
            "        V v;",
        ]
        lines.extend(self._generate_rowview_populate(entity))
        lines.extend([
            "        auto buf = std::make_shared<std::vector<uint8_t>>();",
            "        if (glz::write_beve(v, *buf))",
            "            return nullptr;",
            "        return buf;",
            "    }",
        ])
        return lines

    def _generate_row_view_meta(self, entity: ParsedEntity, mapping_name: str) -> list[str]:
        """Generate glz::meta<Mapping::RowView> specialization (outside namespace)."""
        fqn = f"entity::generated::{mapping_name}"
        return [
            f"template<>",
            f"struct glz::meta<{fqn}::RowView> {{",
            f"    using T = {fqn}::RowView;",
            f"    static constexpr auto value = {fqn}::glaze_value<T>;",
            f"}};",
        ]

    # =========================================================================
    # Embedded ListDescriptor (nested inside Mapping — auto-detected by ListMixin)
    # =========================================================================

    def _generate_embedded_descriptor(self, entity: ParsedEntity) -> list[str]:
        """Generate ListDescriptor struct nested inside the Mapping."""
        a = entity.annotation
        struct_fqn = (f"{entity.namespace}::{entity.class_name}"
                      if entity.namespace else entity.class_name)
        decl_ns = "jcailloux::relais::cache::list::decl"

        limits = a.limits if a.limits else [10, 25, 50]
        default_limit = limits[0]
        max_limit = limits[-1]

        lines = [
            "    // Embedded ListDescriptor — auto-detected by ListMixin",
            "    struct ListDescriptor {",
        ]

        if a.filters:
            # Sort filters alphabetically by param name for deterministic cache keys
            a.filters.sort(key=lambda f: f.param)
            lines.append("")
            lines.append("        static constexpr auto filters = std::tuple{")
            for i, f in enumerate(a.filters):
                comma = "," if i < len(a.filters) - 1 else ""
                f_col = self._col(entity, f.field)
                if f.op == "eq":
                    lines.append(
                        f'            {decl_ns}::Filter<'
                        f'"{f.param}", &{struct_fqn}::{f.field}, "{f_col}"'
                        f'>{{}}{comma}')
                else:
                    op_str = f.op.upper()
                    lines.append(
                        f'            {decl_ns}::Filter<'
                        f'"{f.param}", &{struct_fqn}::{f.field}, "{f_col}", '
                        f'{decl_ns}::Op::{op_str}'
                        f'>{{}}{comma}')
            lines.append("        };")

        if a.sorts:
            lines.append("")
            lines.append("        static constexpr auto sorts = std::tuple{")
            for i, s in enumerate(a.sorts):
                comma = "," if i < len(a.sorts) - 1 else ""
                s_col = self._col(entity, s.field)
                direction = ("SortDirection::Desc" if s.direction == "desc"
                             else "SortDirection::Asc")
                lines.append(
                    f'            {decl_ns}::Sort<'
                    f'"{s.param}", &{struct_fqn}::{s.field}, "{s_col}", '
                    f'{decl_ns}::{direction}'
                    f'>{{}}{comma}')
            lines.append("        };")

        lines.append("")
        lines.append(f"        static constexpr uint16_t defaultLimit = {default_limit};")
        lines.append(f"        static constexpr uint16_t maxLimit = {max_limit};")
        lines.append("    };")

        return lines

    # =========================================================================
    # FieldInfo specializations
    # =========================================================================

    def _generate_field_info(self, entity: ParsedEntity, mapping_name: str,
                              updateable: list[DataMember]) -> list[str]:
        a = entity.annotation
        if not updateable or a.read_only:
            return []

        traits_path = f"{mapping_name}::TraitsType"
        lines = []
        for m in updateable:
            is_timestamp = m.name in a.timestamps
            is_nullable = m.is_optional
            enum_mapping = self._find_enum_mapping(a, m.name)

            # Determine value_type (timestamps are stored as strings)
            if enum_mapping or m.is_raw_json:
                value_type = "std::string"
            elif is_nullable:
                value_type = m.inner_type
            else:
                value_type = m.cpp_type

            lines.append(f"template<>")
            lines.append(f"struct {traits_path}::FieldInfo<{traits_path}::Field::{m.name}> {{")
            lines.append(f"    using value_type = {value_type};")
            lines.append(f'    static constexpr const char* column_name = "\\"{m.col_name}\\"";')
            lines.append(f"    static constexpr bool is_timestamp = {'true' if is_timestamp else 'false'};")
            lines.append(f"    static constexpr bool is_nullable = {'true' if is_nullable else 'false'};")
            lines.append("};")
            lines.append("")

        return lines

    # =========================================================================
    # Enum glz::meta generation
    # =========================================================================

    def _generate_struct_meta(self, entity: ParsedEntity,
                               source_content: str) -> list[str]:
        """Generate glz::meta<Struct> if not already specialized in the source header."""
        struct_fqn = (f"{entity.namespace}::{entity.class_name}"
                      if entity.namespace else entity.class_name)
        bare_name = entity.class_name

        pattern = rf'struct\s+glz::meta\s*<\s*(?:[\w:]+::)*{re.escape(bare_name)}\s*>'
        if re.search(pattern, source_content):
            return []

        lines = [
            f"template<>",
            f"struct glz::meta<{struct_fqn}> {{",
            f"    using T = {struct_fqn};",
            f"    static constexpr auto value = glz::object(",
        ]
        for i, m in enumerate(entity.members):
            comma = "," if i < len(entity.members) - 1 else ""
            lines.append(f'        "{m.name}", &T::{m.name}{comma}')
        lines.append(f"    );")
        lines.append(f"}};")

        return lines

    def _generate_enum_metas(self, entity: ParsedEntity,
                              source_content: str) -> list[str]:
        """Generate glz::meta<EnumType> for enums without existing specializations."""
        a = entity.annotation
        lines = []
        seen_types = set()

        for enum in a.enums:
            if not enum.cpp_type:
                continue
            enum_fqn = self._qualify_type(entity, enum.cpp_type)
            if enum_fqn in seen_types:
                continue
            seen_types.add(enum_fqn)

            bare_name = enum.cpp_type.split("::")[-1]
            pattern = rf'struct\s+glz::meta\s*<\s*(?:[\w:]+::)*{re.escape(bare_name)}\s*>'
            if re.search(pattern, source_content):
                continue

            lines.append(f"template<>")
            lines.append(f"struct glz::meta<{enum_fqn}> {{")
            lines.append(f"    using enum {enum_fqn};")
            lines.append(f"    static constexpr auto value = glz::enumerate(")
            for i, (db_val, enum_val) in enumerate(enum.pairs):
                comma = "," if i < len(enum.pairs) - 1 else ""
                lines.append(f'        "{db_val}", {enum_val}{comma}')
            lines.append(f"    );")
            lines.append(f"}};")

        return lines

    # =========================================================================
    # Helpers
    # =========================================================================

    def _resolve_auto_enums(self, entity: ParsedEntity, source_content: str):
        """Resolve enum pairs from glz::meta<EnumType> for @relais enum."""
        for enum in entity.annotation.enums:
            if enum.pairs:
                continue

            pairs = self._parse_glz_enumerate(source_content, enum.cpp_type, entity)
            if pairs:
                enum.pairs = pairs
            else:
                enum_fqn = self._qualify_type(entity, enum.cpp_type)
                raise ValueError(
                    f"@relais enum on field '{enum.field_name}': "
                    f"no glz::meta<{enum_fqn}> with glz::enumerate() found in "
                    f"{entity.source_file.name}. "
                    f"Either define glz::meta<{enum_fqn}> in the source header, "
                    f"or use explicit mapping: enum=val1:Variant1,val2:Variant2")

    @staticmethod
    def _parse_glz_enumerate(source_content: str, cpp_type: str,
                              entity: ParsedEntity) -> list[tuple[str, str]]:
        """Parse glz::meta<EnumType> from source to extract enumerate pairs."""
        bare_name = cpp_type.split("::")[-1]

        pattern = (
            r'struct\s+glz::meta\s*<\s*(?:[\w:]+::)*'
            + re.escape(bare_name)
            + r'\s*>\s*\{(.*?)\}\s*;'
        )
        match = re.search(pattern, source_content, re.DOTALL)
        if not match:
            return []

        body = match.group(1)

        enum_match = re.search(r'glz::enumerate\((.*?)\)', body, re.DOTALL)
        if not enum_match:
            return []

        enumerate_body = enum_match.group(1)
        return re.findall(r'"([^"]+)"\s*,\s*(\w+)', enumerate_body)

    def _get_updateable_fields(self, entity: ParsedEntity) -> list[DataMember]:
        """Return fields eligible for Field enum (exclude all PKs, db_managed, json_fields)."""
        a = entity.annotation
        pk_set = set(a.primary_keys) if a.primary_keys else {"id"}
        result = []
        for m in entity.members:
            if m.name in pk_set:
                continue
            if m.name in a.db_managed:
                continue
            if m.name in a.json_fields:
                continue
            result.append(m)
        return result

    def _find_enum_mapping(self, a: EntityAnnotation, field_name: str) -> Optional[EnumMapping]:
        for em in a.enums:
            if em.field_name == field_name:
                return em
        return None

    @staticmethod
    def _col(entity: ParsedEntity, field_name: str) -> str:
        """Resolve DB column name for a field (falls back to field name)."""
        for m in entity.members:
            if m.name == field_name:
                return m.col_name
        return field_name

    @staticmethod
    def _qualify_type(entity: ParsedEntity, cpp_type: str) -> str:
        """Qualify a type with the entity's namespace if not already qualified."""
        if "::" in cpp_type:
            return cpp_type
        if entity.namespace:
            return f"{entity.namespace}::{cpp_type}"
        return cpp_type


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate standalone Mapping structs from annotated C++ structs")
    parser.add_argument("--sources", nargs="+", required=True,
                        help="Files and/or directories to scan for @relais annotated headers")
    parser.add_argument("--output-dir", required=True,
                        help="Destination directory for generated wrapper files")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    struct_parser = StructParser()

    # Collect files to process (sources can be a mix of files and directories)
    files: list[Path] = []
    for src in args.sources:
        p = Path(src)
        if p.is_dir():
            for f in p.rglob("*.h"):
                if "generated" in str(f) or "Wrapper" in f.name:
                    continue
                files.append(f)
        elif p.is_file():
            files.append(p)
        else:
            print(f"Error: {src} is not a file or directory", file=sys.stderr)
            sys.exit(1)

    generator = MappingGenerator()
    generated_count = 0

    for filepath in files:
        entities = struct_parser.parse_file(filepath)
        for entity in entities:
            filename = f"{entity.class_name}Wrapper.h"
            output_path = output_dir / filename

            print(f"  -> {entity.class_name}")
            code = generator.generate(entity, output_path)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(code)
            generated_count += 1

    print(f"Generated {generated_count} entity wrappers")


if __name__ == "__main__":
    main()
