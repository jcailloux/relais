#!/usr/bin/env python3
"""
Struct Entity Wrapper Generator

Scans C++ header files for @smartrepo annotations, parses struct data members,
and generates:
  - Standalone Mapping structs with fromModel/toModel/TraitsType/FieldInfo
  - EntityWrapper<Struct, Mapping> type aliases (public API)
  - ListWrapper and ListDescriptor for list entities

Usage:
    python generate_entities.py --scan dir/ --output-dir base/
    python generate_entities.py --files file1.h file2.h --output-dir base/
"""

import argparse
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
    tags: set[str] = field(default_factory=set)
    enum_pairs: list[tuple[str, str]] = field(default_factory=list)
    filter_configs: list[FilterConfig] = field(default_factory=list)
    sort_configs: list[SortConfig] = field(default_factory=list)

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
    field: str      # Struct field name (→ &Entity::field, &Cols::_field)
    op: str = "eq"  # Comparison operator: eq, ne, gt, ge, lt, le


@dataclass
class SortConfig:
    param: str            # HTTP query parameter name
    field: str            # Struct field name
    direction: str = "desc"  # asc or desc


@dataclass
class EntityAnnotation:
    """Parsed @smartrepo annotations for an entity."""
    model: str = ""
    primary_key: str = "id"
    db_managed: list[str] = field(default_factory=list)
    timestamps: list[str] = field(default_factory=list)
    raw_json: list[str] = field(default_factory=list)
    json_fields: list[str] = field(default_factory=list)
    enums: list[EnumMapping] = field(default_factory=list)
    output: str = ""
    read_only: bool = False
    # List annotations
    filters: list[FilterConfig] = field(default_factory=list)
    sorts: list[SortConfig] = field(default_factory=list)
    limits: list[int] = field(default_factory=list)
    entity_fqn: str = ""  # Fully qualified entity class name for Descriptor

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
    """Parse C++ headers for @smartrepo annotations and struct data members."""

    # Regex: @smartrepo or @smartrepo_list annotations in comments
    ANNOTATION_RE = re.compile(r'//\s*@smartrepo(?:_list)?\s+(.*)')
    # Regex: class/struct Name [: public ...] {
    CLASS_RE = re.compile(r'(?:class|struct)\s+(\w+)')
    # Regex: data member — type name [= default]; [// @smartrepo ...]
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

        # Find all @smartrepo annotation blocks followed by a class/struct declaration
        lines = content.split('\n')
        i = 0
        while i < len(lines):
            # Look for @smartrepo annotation block
            annotations = []
            while i < len(lines) and self.ANNOTATION_RE.match(lines[i].strip()):
                m = self.ANNOTATION_RE.match(lines[i].strip())
                annotations.append((
                    '@smartrepo_list' in lines[i],
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
                    if token.startswith("model="):
                        annot.model = token[6:]
                    elif token.startswith("output="):
                        annot.output = token[7:]
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
                # Inside key=value, check if complete
                # enum values contain commas/colons but no spaces after the value
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
        # Remaining parts are val:Enum pairs joined by commas
        rest = ":".join(parts[1:])
        pairs = []
        for pair_str in rest.split(","):
            pair_parts = pair_str.split(":")
            if len(pair_parts) == 2:
                pairs.append((pair_parts[0], pair_parts[1]))
        return EnumMapping(field_name, pairs)

    def _apply_member_annotations(self, annot: EntityAnnotation,
                                    members: list[DataMember]):
        """Populate EntityAnnotation fields from inline member @smartrepo tags."""
        for m in members:
            if 'primary_key' in m.tags:
                annot.primary_key = m.name
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
            filterable              → EQ on field_name
            filterable:custom_name  → EQ with custom HTTP param
            filterable:ge           → GE operator (known op) on field_name
            filterable:date_from:ge → custom param + GE operator
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
            sortable            → DESC (default)
            sortable:asc        → ASC
            sortable:desc       → DESC
            sortable:name:asc   → custom param + ASC
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
        # Find all namespace declarations before the class line
        pos = content.find(class_line)
        if pos < 0:
            return ""
        before = content[:pos]
        matches = list(self.NAMESPACE_RE.finditer(before))
        return matches[-1].group(1) if matches else ""

    def _parse_members(self, lines: list[str], class_line_idx: int) -> list[DataMember]:
        """Parse data members from a class/struct body."""
        members = []
        # Find opening brace
        brace_depth = 0
        in_public = False
        is_struct = 'struct ' in lines[class_line_idx]

        for i in range(class_line_idx, len(lines)):
            line = lines[i]
            if '{' in line:
                brace_depth += line.count('{') - line.count('}')
                # For structs, default access is public
                if i == class_line_idx and is_struct:
                    in_public = True
                continue

            if brace_depth == 0 and i > class_line_idx:
                break  # Past the class body

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

            # Skip function declarations and special members
            if "[[nodiscard]]" in stripped or "(" in stripped:
                continue
            if stripped.startswith("//") or stripped.startswith("using "):
                continue
            if not stripped or stripped == "};":
                continue

            # Try to match a data member (with optional inline @smartrepo annotation)
            m = self.MEMBER_RE.match(line)
            if m:
                cpp_type = m.group(1).strip()
                name = m.group(2)
                default = m.group(3).strip() if m.group(3) else ""
                tags = set()
                enum_pairs = []
                flt_configs = []
                srt_configs = []
                trailing = m.group(4).strip() if m.group(4) else ""
                if '@smartrepo' in trailing:
                    idx = trailing.index('@smartrepo') + len('@smartrepo')
                    tag_text = trailing[idx:].strip()
                    for tag in tag_text.split():
                        if tag.startswith('enum='):
                            for pair in tag[5:].split(','):
                                parts = pair.split(':')
                                if len(parts) == 2:
                                    enum_pairs.append((parts[0], parts[1]))
                        elif tag == 'enum':
                            tags.add('auto_enum')
                        elif tag.startswith('filterable'):
                            flt_configs.append(
                                self._parse_filterable_tag(name, tag))
                        elif tag.startswith('sortable'):
                            srt_configs.append(
                                self._parse_sortable_tag(name, tag))
                        else:
                            tags.add(tag)
                members.append(DataMember(
                    name, cpp_type, default, tags, enum_pairs,
                    flt_configs, srt_configs))

        return members


# =============================================================================
# Code Generator
# =============================================================================

class MappingGenerator:
    """Generate Mapping structs + EntityWrapper aliases from parsed entities."""

    def __init__(self, output_dir: Path, scan_dirs: list[Path]):
        self.output_dir = output_dir
        self.scan_dirs = scan_dirs

    def generate(self, entity: ParsedEntity) -> str:
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
        lines.extend(self._generate_includes(entity))
        lines.append("")
        lines.append("namespace entity::generated {")
        lines.append("")

        # Mapping struct (contains TraitsType, getPrimaryKey, fromModel, toModel)
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
        lines.append(f"using {wrapper_name} = jcailloux::drogon::wrapper::EntityWrapper<")
        lines.append(f"    {struct_fqn}, {mapping_name}>;")

        # === List wrapper + descriptor (if list annotations present) ===
        if a.has_list:
            list_wrapper_name = f"{entity.class_name}ListWrapper"
            lines.append("")
            lines.append(f"using {list_wrapper_name} = jcailloux::drogon::wrapper::ListWrapper<{wrapper_name}>;")
            # Note: ListDescriptor is now embedded inside the Mapping struct
            # (generated by _generate_embedded_descriptor). The standalone
            # _generate_descriptor() is kept for reference but no longer called.

        lines.append("")
        lines.append("}  // namespace entity::generated")

        # === glz::meta for struct and enums (outside namespace) ===
        # Skipped when the developer defines their own in the source header.
        struct_meta = self._generate_struct_meta(entity, source_content)
        enum_metas = self._generate_enum_metas(entity, source_content)
        if struct_meta:
            lines.append("")
            lines.extend(struct_meta)
        if enum_metas:
            lines.append("")
            lines.extend(enum_metas)

        lines.append("")

        return "\n".join(lines)

    # =========================================================================
    # Includes
    # =========================================================================

    def _generate_includes(self, entity: ParsedEntity) -> list[str]:
        a = entity.annotation
        lines = [
            "#include <cstdint>",
            "#include <optional>",
            "#include <string>",
        ]

        # Trantor for timestamps
        if a.timestamps:
            lines.append("#include <trantor/utils/Date.h>")

        # Glaze for json_fields deserialization
        if a.json_fields:
            lines.append("#include <glaze/glaze.hpp>")

        # EntityWrapper (always needed for the wrapper alias)
        lines.append("#include <jcailloux/drogon/wrapper/EntityWrapper.h>")

        # Struct header (needed for the EntityWrapper alias)
        struct_include = self._find_struct_include(entity)
        lines.append(f'#include "{struct_include}"')

        # Model header
        if a.model:
            model_include = self._find_model_include(a.model)
            lines.append(f'#include "{model_include}"')

        # List includes
        if a.has_list:
            lines.append("#include <array>")
            lines.append("#include <jcailloux/drogon/wrapper/ListWrapper.h>")
            lines.append("#include <jcailloux/drogon/list/decl/FilterDescriptor.h>")
            lines.append("#include <jcailloux/drogon/list/decl/SortDescriptor.h>")

        return lines

    def _find_struct_include(self, entity: ParsedEntity) -> str:
        """Return include path for the struct header, relative to output_dir."""
        try:
            return str(entity.source_file.relative_to(self.output_dir))
        except ValueError:
            return str(entity.source_file)

    def _find_model_include(self, model_class: str) -> str:
        """Find model header and return include path relative to output_dir."""
        header_name = model_class.split("::")[-1] + ".h"
        for scan_dir in self.scan_dirs:
            for path in scan_dir.rglob(header_name):
                try:
                    return str(path.relative_to(self.output_dir))
                except ValueError:
                    return str(path)
        # Fallback: just use the class name
        return f"{header_name}"

    # =========================================================================
    # Mapping struct (standalone — used by EntityWrapper<Struct, Mapping>)
    # =========================================================================

    def _generate_mapping_struct(self, entity: ParsedEntity, mapping_name: str,
                                   updateable: list[DataMember]) -> list[str]:
        a = entity.annotation
        lines = [
            f"struct {mapping_name} {{",
            f"    using Model = {a.model};",
            f"    static constexpr bool read_only = {'true' if a.read_only else 'false'};",
            "",
        ]

        # Nested TraitsType
        lines.extend(self._generate_traits_type(entity, updateable))
        lines.append("")

        # getPrimaryKey (template — deferred to instantiation)
        lines.append("    template<typename Entity>")
        lines.append("    static auto getPrimaryKey(const Entity& e) noexcept {")
        lines.append(f"        return e.{a.primary_key};")
        lines.append("    }")
        lines.append("")

        # fromModel
        lines.extend(self._generate_from_model(entity))

        # toModel (skip for read-only)
        if not a.read_only:
            lines.append("")
            lines.extend(self._generate_to_model(entity))

        # makeKeyCriteria (when primary key is db_managed — partitioned tables)
        if a.primary_key in a.db_managed:
            lines.append("")
            lines.extend(self._generate_make_key_criteria(entity))

        # Glaze metadata template (used by EntityWrapper's glz::meta delegation)
        lines.append("")
        lines.extend(self._generate_glaze_value(entity))

        # Embedded ListDescriptor (if list annotations present)
        if a.has_list:
            lines.append("")
            lines.extend(self._generate_embedded_descriptor(entity))

        lines.append("};")
        return lines

    # =========================================================================
    # TraitsType (nested inside Mapping)
    # =========================================================================

    def _generate_traits_type(self, entity: ParsedEntity,
                               updateable: list[DataMember]) -> list[str]:
        a = entity.annotation
        lines = [
            "    struct TraitsType {",
            f"        using Model = {a.model};",
        ]

        if updateable and not a.read_only:
            lines.append("")
            lines.append("        enum class Field : uint8_t {")
            for i, m in enumerate(updateable):
                comma = "," if i < len(updateable) - 1 else ""
                lines.append(f"            {m.name}{comma}")
            lines.append("        };")
            lines.append("")
            lines.append("        template<Field> struct FieldInfo;")

        lines.append("")
        pk_setter = self._model_setter(a.primary_key)
        lines.append(f"        static void setPrimaryKeyOnModel(Model& model, const auto& key) {{")
        lines.append(f"            model.{pk_setter}(key);")
        lines.append("        }")
        lines.append("    };")
        return lines

    # =========================================================================
    # fromModel / toModel (template methods inside Mapping struct)
    # =========================================================================

    def _generate_from_model(self, entity: ParsedEntity) -> list[str]:
        a = entity.annotation
        lines = [
            "    template<typename Entity>",
            "    static std::optional<Entity> fromModel(const Model& model) {",
            "        Entity e;",
        ]

        for m in entity.members:
            getter = self._model_getter(m.name)
            enum_mapping = self._find_enum_mapping(a, m.name)

            if m.name in a.json_fields:
                # JSON field: deserialize from string column
                if m.is_optional:
                    lines.append(f"        {{")
                    lines.append(f"            auto json_str = std::string(model.{getter}());")
                    lines.append(f"            if (!json_str.empty()) {{")
                    lines.append(f"                typename std::remove_reference_t<decltype(*e.{m.name})> tmp;")
                    lines.append(f"                if (!glz::read_json(tmp, json_str))")
                    lines.append(f"                    e.{m.name} = std::move(tmp);")
                    lines.append(f"            }}")
                    lines.append(f"        }}")
                else:
                    lines.append(f"        glz::read_json(e.{m.name}, std::string(model.{getter}()));")
            elif m.is_raw_json:
                # Raw JSON: store as glz::raw_json (auto-detected from type)
                lines.append(f"        e.{m.name}.str = model.{getter}();")
            elif m.name in a.timestamps:
                # Timestamp: convert Date -> string
                if m.is_optional:
                    null_check = self._model_null_check(m.name)
                    lines.append(f"        if (auto sp = model.{null_check}())")
                    lines.append(f"            e.{m.name} = sp->toDbStringLocal();")
                else:
                    lines.append(f"        e.{m.name} = model.{getter}().toDbStringLocal();")
            elif enum_mapping:
                # Enum: convert string -> enum (FQN needed — Mapping is in entity::generated)
                enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                if m.is_optional:
                    null_check = self._model_null_check(m.name)
                    lines.append(f"        if (auto sp = model.{null_check}()) {{")
                    lines.append(f"            const auto& s = *sp;")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") e.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
                else:
                    lines.append(f"        {{")
                    lines.append(f"            const auto& s = model.{getter}();")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            if (s == "{db_val}") e.{m.name} = {enum_fqn}::{enum_val};')
                    lines.append(f"        }}")
            elif m.is_optional:
                # Nullable scalar/string: check shared_ptr
                null_check = self._model_null_check(m.name)
                lines.append(f"        if (auto sp = model.{null_check}())")
                lines.append(f"            e.{m.name} = *sp;")
            else:
                # Simple field: direct assignment
                lines.append(f"        e.{m.name} = model.{getter}();")

        lines.append("        return e;")
        lines.append("    }")
        return lines

    def _generate_to_model(self, entity: ParsedEntity) -> list[str]:
        a = entity.annotation
        lines = [
            "    template<typename Entity>",
            "    static Model toModel(const Entity& e) {",
            "        Model m;",
        ]

        for m in entity.members:
            # Skip db_managed fields (auto-increment, etc.)
            if m.name in a.db_managed:
                lines.append(f"        // {m.name}: skipped (db_managed)")
                continue

            # Skip json_fields and composites not stored directly
            setter = self._model_setter(m.name)
            enum_mapping = self._find_enum_mapping(a, m.name)

            if m.name in a.json_fields:
                # JSON field: serialize to string column
                if m.is_optional:
                    lines.append(f"        if (e.{m.name}) {{")
                    lines.append(f"            std::string json;")
                    lines.append(f"            glz::write_json(*e.{m.name}, json);")
                    lines.append(f"            m.{setter}(json);")
                    lines.append(f"        }}")
                else:
                    lines.append(f"        {{ std::string json; glz::write_json(e.{m.name}, json); m.{setter}(json); }}")
            elif m.is_raw_json:
                # Raw JSON: get string from glz::raw_json (auto-detected from type)
                lines.append(f"        m.{setter}(e.{m.name}.str);")
            elif m.name in a.timestamps:
                # Timestamp: convert string -> Date
                if m.is_optional:
                    set_null = self._model_setter(m.name) + "ToNull"
                    lines.append(f"        if (e.{m.name}.has_value())")
                    lines.append(f"            m.{setter}(::trantor::Date::fromDbStringLocal(*e.{m.name}));")
                    lines.append(f"        else")
                    lines.append(f"            m.{set_null}();")
                else:
                    lines.append(f"        m.{setter}(::trantor::Date::fromDbStringLocal(e.{m.name}));")
            elif enum_mapping:
                # Enum: convert enum -> string (FQN needed)
                enum_fqn = self._qualify_type(entity, enum_mapping.cpp_type)
                if m.is_optional:
                    set_null = self._model_setter(m.name) + "ToNull"
                    lines.append(f"        if (e.{m.name}.has_value()) {{")
                    lines.append(f"            switch (*e.{m.name}) {{")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'                case {enum_fqn}::{enum_val}: m.{setter}("{db_val}"); break;')
                    lines.append(f"            }}")
                    lines.append(f"        }} else {{")
                    lines.append(f"            m.{set_null}();")
                    lines.append(f"        }}")
                else:
                    lines.append(f"        switch (e.{m.name}) {{")
                    for db_val, enum_val in enum_mapping.pairs:
                        lines.append(f'            case {enum_fqn}::{enum_val}: m.{setter}("{db_val}"); break;')
                    lines.append(f"        }}")
            elif m.is_optional:
                # Nullable: conditional set or setToNull
                set_null = self._model_setter(m.name) + "ToNull"
                lines.append(f"        if (e.{m.name}.has_value())")
                lines.append(f"            m.{setter}(*e.{m.name});")
                lines.append(f"        else")
                lines.append(f"            m.{set_null}();")
            else:
                # Simple field
                lines.append(f"        m.{setter}(e.{m.name});")

        lines.append("        return m;")
        lines.append("    }")
        return lines

    # =========================================================================
    # Glaze metadata (variable template inside Mapping)
    # =========================================================================

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
    # makeKeyCriteria (for partitioned tables — PK is db_managed)
    # =========================================================================

    def _generate_make_key_criteria(self, entity: ParsedEntity) -> list[str]:
        """Generate makeKeyCriteria template method for partial-key entities."""
        a = entity.annotation
        col = f"_{a.primary_key}"
        return [
            "    template<typename M>",
            "    static auto makeKeyCriteria(const auto& key) {",
            f"        return ::drogon::orm::Criteria(M::Cols::{col}, key);",
            "    }",
        ]

    # =========================================================================
    # Embedded ListDescriptor (nested inside Mapping — auto-detected by ListMixin)
    # =========================================================================

    def _generate_embedded_descriptor(self, entity: ParsedEntity) -> list[str]:
        """Generate ListDescriptor struct nested inside the Mapping."""
        a = entity.annotation
        struct_fqn = (f"{entity.namespace}::{entity.class_name}"
                      if entity.namespace else entity.class_name)
        decl_ns = "jcailloux::drogon::cache::list::decl"

        limits = a.limits if a.limits else [10, 25, 50]
        default_limit = limits[0]
        max_limit = limits[-1]
        limits_str = ", ".join(str(l) for l in limits)

        lines = [
            "    // Embedded ListDescriptor — auto-detected by ListMixin",
            "    struct ListDescriptor {",
            "        using Cols = Model::Cols;",
            "",
        ]

        if a.filters:
            lines.append("        static constexpr auto filters = std::make_tuple(")
            for i, f in enumerate(a.filters):
                comma = "," if i < len(a.filters) - 1 else ""
                col = f"_{f.field}"
                if f.op == "eq":
                    lines.append(
                        f'            {decl_ns}::Filter<"{f.param}", '
                        f'&{struct_fqn}::{f.field}, &Cols::{col}>{{}}{comma}')
                else:
                    op_str = f.op.upper()
                    lines.append(
                        f'            {decl_ns}::Filter<"{f.param}", '
                        f'&{struct_fqn}::{f.field}, &Cols::{col}, '
                        f'{decl_ns}::Op::{op_str}>{{}}{comma}')
            lines.append("        );")
            lines.append("")

        if a.sorts:
            lines.append("        static constexpr auto sorts = std::make_tuple(")
            for i, s in enumerate(a.sorts):
                comma = "," if i < len(a.sorts) - 1 else ""
                col = f"_{s.field}"
                direction = ("SortDirection::Desc" if s.direction == "desc"
                             else "SortDirection::Asc")
                model_getter = self._model_getter(s.field)
                lines.append(
                    f'            {decl_ns}::Sort<"{s.param}", '
                    f'&{struct_fqn}::{s.field}, &Cols::{col}, '
                    f'&Model::{model_getter}, {decl_ns}::{direction}>{{}}{comma}')
            lines.append("        );")
            lines.append("")

        lines.append(f"        static constexpr std::array<uint16_t, {len(limits)}>"
                     f" allowedLimits = {{{limits_str}}};")
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

            # Determine value_type and setter
            is_raw_json = m.is_raw_json
            is_vector_char = m.cpp_type == "std::vector<char>" or m.inner_type == "std::vector<char>"
            if is_timestamp:
                value_type = "::trantor::Date"
                setter_ref = f"&Model::{self._model_setter(m.name)}"
            elif is_raw_json or enum_mapping or m.is_string:
                # raw_json: DB stores as string, model setter takes string
                value_type = "std::string"
                setter_name = self._model_setter(m.name)
                setter_ref = f"static_cast<void(Model::*)(const std::string&)>(&Model::{setter_name})"
            elif is_vector_char:
                # vector<char>: Drogon has overloaded setter (vector<char> + string), disambiguate
                value_type = "std::vector<char>"
                setter_name = self._model_setter(m.name)
                setter_ref = f"static_cast<void(Model::*)(const std::vector<char>&)>(&Model::{setter_name})"
            elif is_nullable:
                value_type = m.inner_type
                setter_ref = f"&Model::{self._model_setter(m.name)}"
            else:
                value_type = m.cpp_type
                setter_ref = f"&Model::{self._model_setter(m.name)}"

            lines.append(f"template<>")
            lines.append(f"struct {traits_path}::FieldInfo<{traits_path}::Field::{m.name}> {{")
            lines.append(f"    using value_type = {value_type};")
            lines.append(f"    static constexpr auto setter = {setter_ref};")
            lines.append(f'    static constexpr const char* column_name = "\\"{m.name}\\"";')
            lines.append(f"    static constexpr bool is_timestamp = {'true' if is_timestamp else 'false'};")
            lines.append(f"    static constexpr bool is_nullable = {'true' if is_nullable else 'false'};")
            if is_nullable:
                set_null_name = self._model_setter(m.name) + "ToNull"
                lines.append(f"    static constexpr auto setToNull = &Model::{set_null_name};")
            lines.append("};")
            lines.append("")

        return lines

    # =========================================================================
    # List descriptor (for list entities)
    # =========================================================================

    def _generate_descriptor(self, entity: ParsedEntity, wrapper_name: str) -> list[str]:
        a = entity.annotation
        descriptor_name = f"{entity.class_name}ListDescriptor"
        list_wrapper_name = f"{entity.class_name}ListWrapper"
        decl_ns = "jcailloux::drogon::cache::list::decl"

        limits = a.limits if a.limits else [10, 25, 50]
        default_limit = limits[0]
        max_limit = limits[-1]
        limits_str = ", ".join(str(l) for l in limits)

        lines = [
            "// ============================================================================",
            f"// {descriptor_name} - Declarative list configuration",
            "// ============================================================================",
            "",
            f"struct {descriptor_name} {{",
            f"    using Entity = {wrapper_name};",
            f"    using Model = {a.model};",
            f"    using ListEntity = {list_wrapper_name};",
            "    using Key = int64_t;",
            "    using Cols = Model::Cols;",
            "",
        ]

        if a.filters:
            lines.append("    static constexpr auto filters = std::make_tuple(")
            for i, f in enumerate(a.filters):
                comma = "," if i < len(a.filters) - 1 else ""
                col = f"_{f.field}"
                if f.op == "eq":
                    lines.append(f'        {decl_ns}::Filter<"{f.param}", &Entity::{f.field}, &Cols::{col}>{{}}{comma}')
                else:
                    op_str = f.op.upper()
                    lines.append(f'        {decl_ns}::Filter<"{f.param}", &Entity::{f.field}, &Cols::{col}, {decl_ns}::Op::{op_str}>{{}}{comma}')
            lines.append("    );")
            lines.append("")

        if a.sorts:
            lines.append("    static constexpr auto sorts = std::make_tuple(")
            for i, s in enumerate(a.sorts):
                comma = "," if i < len(a.sorts) - 1 else ""
                col = f"_{s.field}"
                direction = "SortDirection::Desc" if s.direction == "desc" else "SortDirection::Asc"
                model_getter = self._model_getter(s.field)
                lines.append(f'        {decl_ns}::Sort<"{s.param}", &Entity::{s.field}, &Cols::{col}, &Model::{model_getter}, {decl_ns}::{direction}>{{}}{comma}')
            lines.append("    );")
            lines.append("")

        lines.append(f"    static constexpr std::array<uint16_t, {len(limits)}> allowedLimits = {{{limits_str}}};")
        lines.append(f"    static constexpr uint16_t defaultLimit = {default_limit};")
        lines.append(f"    static constexpr uint16_t maxLimit = {max_limit};")
        lines.append("};")

        return lines

    # =========================================================================
    # Enum glz::meta generation
    # =========================================================================

    def _generate_struct_meta(self, entity: ParsedEntity,
                               source_content: str) -> list[str]:
        """Generate glz::meta<Struct> if not already specialized in the source header.

        Ensures a single serialization contract for the struct, whether it's
        serialized standalone (via EntityWrapper) or nested (via json_field
        in another entity). Without this, nested serialization relies on
        Glaze auto-reflection, which could diverge from Mapping::glaze_value.
        """
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
        """Generate glz::meta<EnumType> for enums without existing specializations.

        Scans the source header for existing glz::meta<EnumType> specializations.
        If found, the developer owns it — skip generation. Otherwise, generate
        from the @smartrepo enum= annotation pairs.
        """
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

            # Detect existing glz::meta<EnumType> in the source header
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
        """Resolve enum pairs from glz::meta<EnumType> for @smartrepo enum (without explicit mapping)."""
        for enum in entity.annotation.enums:
            if enum.pairs:
                continue  # Explicit mapping provided, skip

            pairs = self._parse_glz_enumerate(source_content, enum.cpp_type, entity)
            if pairs:
                enum.pairs = pairs
            else:
                enum_fqn = self._qualify_type(entity, enum.cpp_type)
                raise ValueError(
                    f"@smartrepo enum on field '{enum.field_name}': "
                    f"no glz::meta<{enum_fqn}> with glz::enumerate() found in "
                    f"{entity.source_file.name}. "
                    f"Either define glz::meta<{enum_fqn}> in the source header, "
                    f"or use explicit mapping: enum=val1:Variant1,val2:Variant2")

    @staticmethod
    def _parse_glz_enumerate(source_content: str, cpp_type: str,
                              entity: ParsedEntity) -> list[tuple[str, str]]:
        """Parse glz::meta<EnumType> from source to extract enumerate pairs."""
        bare_name = cpp_type.split("::")[-1]

        # Match: struct glz::meta<...EnumType> { ... };
        pattern = (
            r'struct\s+glz::meta\s*<\s*(?:[\w:]+::)*'
            + re.escape(bare_name)
            + r'\s*>\s*\{(.*?)\}\s*;'
        )
        match = re.search(pattern, source_content, re.DOTALL)
        if not match:
            return []

        body = match.group(1)

        # Match: glz::enumerate( ... )
        enum_match = re.search(r'glz::enumerate\((.*?)\)', body, re.DOTALL)
        if not enum_match:
            return []

        enumerate_body = enum_match.group(1)

        # Extract "string", EnumValue pairs
        return re.findall(r'"([^"]+)"\s*,\s*(\w+)', enumerate_body)

    def _get_updateable_fields(self, entity: ParsedEntity) -> list[DataMember]:
        """Return fields eligible for Field enum (exclude PK, db_managed, json_fields)."""
        a = entity.annotation
        result = []
        for m in entity.members:
            if m.name == a.primary_key:
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
    def _qualify_type(entity: ParsedEntity, cpp_type: str) -> str:
        """Qualify a type with the entity's namespace if not already qualified."""
        if "::" in cpp_type:
            return cpp_type  # Already qualified
        if entity.namespace:
            return f"{entity.namespace}::{cpp_type}"
        return cpp_type

    @staticmethod
    def _model_getter(field_name: str) -> str:
        parts = field_name.split("_")
        camel = "".join(p.capitalize() for p in parts)
        return f"getValueOf{camel}"

    @staticmethod
    def _model_setter(field_name: str) -> str:
        parts = field_name.split("_")
        camel = "".join(p.capitalize() for p in parts)
        return f"set{camel}"

    @staticmethod
    def _model_null_check(field_name: str) -> str:
        parts = field_name.split("_")
        camel = "".join(p.capitalize() for p in parts)
        return f"get{camel}"


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate standalone Mapping structs from annotated C++ structs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scan", nargs="+",
                       help="Directories to scan for @smartrepo annotated headers")
    group.add_argument("--files", nargs="+",
                       help="Specific header files to process")
    parser.add_argument("--output-dir", required=True,
                        help="Base directory for output files")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    struct_parser = StructParser()

    # Collect files to process
    files: list[Path] = []
    scan_dirs: list[Path] = []
    if args.scan:
        for d in args.scan:
            scan_dir = Path(d)
            scan_dirs.append(scan_dir)
            for f in scan_dir.rglob("*.h"):
                # Skip generated files
                if "generated" in str(f) or "Wrapper" in f.name:
                    continue
                files.append(f)
    elif args.files:
        for f in args.files:
            files.append(Path(f))
        # Use file parents as scan dirs
        scan_dirs = list(set(f.parent for f in files))

    generator = MappingGenerator(output_dir, scan_dirs)
    generated_count = 0

    for filepath in files:
        entities = struct_parser.parse_file(filepath)
        for entity in entities:
            if not entity.annotation.output:
                print(f"  Warning: {entity.class_name} has no output annotation, skipping",
                      file=sys.stderr)
                continue

            print(f"  -> {entity.class_name}")
            code = generator.generate(entity)

            output_path = output_dir / entity.annotation.output
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(code)
            generated_count += 1

    print(f"Generated {generated_count} entity wrappers")


if __name__ == "__main__":
    main()
