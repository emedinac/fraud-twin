"""Sphinx directive for rendering Pydantic configuration schemas.

The directive deliberately consumes the model's JSON schema instead of
duplicating configuration metadata in RST.  This keeps the reference table
aligned with the validation code while leaving narrative guidance in the
configuration guide.
"""

import importlib
import json
from collections.abc import Iterable, Mapping
from typing import Any

from docutils import nodes
from docutils.parsers.rst import Directive, directives


def _load_model(path: str) -> Any:
    """Load a dotted model path in ``module:attribute`` or dotted form."""

    module_name, separator, attribute = path.partition(":")
    if not separator:
        module_name, _, attribute = path.rpartition(".")
    if not module_name or not attribute:
        raise ValueError("model path must look like module:Model or module.Model")
    module = importlib.import_module(module_name)
    model = getattr(module, attribute)
    if not hasattr(model, "model_json_schema"):
        raise TypeError(f"{path} is not a Pydantic model")
    return model


def _display(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, dict | list | tuple):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _schema_type(schema: Mapping[str, Any], definitions: Mapping[str, Any]) -> str:
    reference = schema.get("$ref")
    if isinstance(reference, str):
        name = reference.rsplit("/", 1)[-1]
        return name
    variants = schema.get("anyOf") or schema.get("oneOf")
    if isinstance(variants, list):
        return " | ".join(_schema_type(item, definitions) for item in variants)
    if schema.get("type") == "array":
        item_schema = schema.get("items", {})
        return f"list[{_schema_type(item_schema, definitions)}]"
    if schema.get("type") == "object" and "additionalProperties" in schema:
        value_schema = schema["additionalProperties"]
        return f"dict[str, {_schema_type(value_schema, definitions)}]"
    return str(schema.get("type", schema.get("title", "object")))


def _constraints(schema: Mapping[str, Any]) -> str:
    values: list[str] = []
    if "enum" in schema:
        values.append("allowed: " + ", ".join(_display(value) for value in schema["enum"]))
    labels = (
        ("minimum", ">= "),
        ("exclusiveMinimum", "> "),
        ("maximum", "<= "),
        ("exclusiveMaximum", "< "),
        ("minLength", "min length "),
        ("maxLength", "max length "),
        ("minItems", "min items "),
        ("maxItems", "max items "),
        ("pattern", "pattern "),
    )
    for key, label in labels:
        if key in schema:
            values.append(label + _display(schema[key]))
    return "; ".join(values) or "—"


def _xref(name: str) -> nodes.Node:
    reference = nodes.reference("", refuri=f"#config-model-{name.lower()}")
    reference += nodes.literal(name, name)
    return reference


def _type_cell(schema: Mapping[str, Any], definitions: Mapping[str, Any]) -> nodes.Node:
    reference = schema.get("$ref")
    if isinstance(reference, str):
        paragraph = nodes.paragraph()
        paragraph += _xref(reference.rsplit("/", 1)[-1])
        return paragraph
    variants = schema.get("anyOf") or schema.get("oneOf")
    if isinstance(variants, list):
        paragraph = nodes.paragraph()
        for index, item in enumerate(variants):
            if index:
                paragraph += nodes.Text(" | ")
            if isinstance(item.get("$ref"), str):
                paragraph += _xref(item["$ref"].rsplit("/", 1)[-1])
            else:
                paragraph += nodes.literal("", _schema_type(item, definitions))
        return paragraph
    return nodes.literal("", _schema_type(schema, definitions))


def _table(
    properties: Mapping[str, Any],
    required: Iterable[str],
    definitions: Mapping[str, Any],
) -> nodes.table:
    required_names = set(required)
    table = nodes.table(classes=["config-schema-table"])
    tgroup = nodes.tgroup(cols=6)
    table += tgroup
    for width in (18, 12, 18, 18, 22, 24):
        tgroup += nodes.colspec(colwidth=width)
    header = nodes.row()
    for title in (
        "Field",
        "Required",
        "Type",
        "Default",
        "Constraints",
        "Description",
    ):
        header += nodes.entry("", nodes.paragraph("", title))
    tgroup += nodes.thead("", header)
    tbody = nodes.tbody()
    for name, schema in properties.items():
        schema = schema if isinstance(schema, Mapping) else {}
        row = nodes.row()
        row += nodes.entry("", nodes.literal("", name))
        row += nodes.entry("", nodes.paragraph("", "yes" if name in required_names else "no"))
        row += nodes.entry("", _type_cell(schema, definitions))
        row += nodes.entry("", nodes.paragraph("", _display(schema.get("default"))))
        row += nodes.entry("", nodes.paragraph("", _constraints(schema)))
        row += nodes.entry("", nodes.paragraph("", str(schema.get("description", ""))))
        tbody += row
    tgroup += tbody
    return table


class ConfigModelDirective(Directive):
    """Render a Pydantic model and its nested definitions as parameter tables."""

    required_arguments = 1
    optional_arguments = 0
    has_content = False
    option_spec = {"caption": directives.unchanged}

    def run(self) -> list[nodes.Node]:
        path = self.arguments[0].strip()
        try:
            model = _load_model(path)
            schema = model.model_json_schema()
        except (AttributeError, ImportError, TypeError, ValueError) as exc:
            raise self.error(f"unable to load configuration model {path!r}: {exc}") from exc

        definitions = schema.get("$defs", {})
        output: list[nodes.Node] = []
        title = schema.get("title", model.__name__)
        output.append(nodes.title(title, title))
        output.append(
            nodes.paragraph(
                "",
                schema.get("description", f"Configuration fields for {title}."),
            )
        )
        output.append(_table(schema.get("properties", {}), schema.get("required", ()), definitions))

        referenced: list[str] = []
        for property_schema in schema.get("properties", {}).values():
            if isinstance(property_schema, Mapping):
                candidates = [property_schema, *(property_schema.get("anyOf", ()),)]
                for candidate in candidates:
                    reference = candidate.get("$ref") if isinstance(candidate, Mapping) else None
                    if isinstance(reference, str):
                        name = reference.rsplit("/", 1)[-1]
                        if name in definitions and name not in referenced:
                            referenced.append(name)

        pending = list(referenced)
        seen: set[str] = set()
        while pending:
            name = pending.pop(0)
            if name in seen or name not in definitions:
                continue
            seen.add(name)
            definition = definitions[name]
            output.append(nodes.subtitle(name, name, ids=[f"config-model-{name.lower()}"]))
            output.append(
                _table(
                    definition.get("properties", {}),
                    definition.get("required", ()),
                    definitions,
                )
            )
            for nested in definition.get("properties", {}).values():
                if not isinstance(nested, Mapping):
                    continue
                for candidate in [nested, *(nested.get("anyOf", ()),)]:
                    reference = candidate.get("$ref") if isinstance(candidate, Mapping) else None
                    if isinstance(reference, str):
                        nested_name = reference.rsplit("/", 1)[-1]
                        if nested_name not in seen and nested_name in definitions:
                            pending.append(nested_name)
        return output


def setup(app: Any) -> dict[str, Any]:
    app.add_directive("config-model", ConfigModelDirective)
    return {"version": "1", "parallel_read_safe": True}
