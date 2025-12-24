import inspect
from dagster import ConfigurableResource
import dagster_dag_factory.resources as resources_module
from dagster_dag_factory.factory.registry import OperatorRegistry


def generate_docs(output_path: str):
    """
    Generates a Markdown reference for all available resources and operators.
    """
    lines = ["# Dagster Factory Reference\n"]

    # 1. Resources
    lines.append("## Resources\n")
    lines.append("Resources are used to define connections to external systems.\n")

    for name, obj in inspect.getmembers(resources_module):
        if (
            inspect.isclass(obj)
            and issubclass(obj, ConfigurableResource)
            and obj != ConfigurableResource
        ):
            lines.append(f"### `{name}`")
            if obj.__doc__:
                lines.append(f"{obj.__doc__.strip()}\n")

            lines.append("| Field | Type | Description |")
            lines.append("| :--- | :--- | :--- |")

            # Use Pydantic schema to get fields and descriptions
            schema = obj.model_json_schema()
            properties = schema.get("properties", {})
            for field_name, field_def in properties.items():
                f_type = field_def.get("type", "Any")
                f_desc = field_def.get("description", "")
                lines.append(f"| `{field_name}` | `{f_type}` | {f_desc} |")
            lines.append("\n")

    # 2. Operators
    lines.append("## Operators\n")
    lines.append("Operators define how data moves between a source and a target.\n")

    for (source, target), op_class in OperatorRegistry._registry.items():
        lines.append(f"### `{source}` to `{target}` (`{op_class.__name__}`)")
        if op_class.__doc__:
            lines.append(f"{op_class.__doc__.strip()}\n")

        if op_class.source_config_schema:
            lines.append("#### Source Configuration")
            lines.append("| Field | Type | Description |")
            lines.append("| :--- | :--- | :--- |")
            props = op_class.source_config_schema.model_json_schema().get(
                "properties", {}
            )
            for name, d in props.items():
                lines.append(
                    f"| `{name}` | `{d.get('type', 'Any')}` | {d.get('description', '')}|"
                )
            lines.append("\n")

        if op_class.target_config_schema:
            lines.append("#### Target Configuration")
            lines.append("| Field | Type | Description |")
            lines.append("| :--- | :--- | :--- |")
            props = op_class.target_config_schema.model_json_schema().get(
                "properties", {}
            )
            for name, d in props.items():
                lines.append(
                    f"| `{name}` | `{d.get('type', 'Any')}` | {d.get('description', '')}|"
                )
            lines.append("\n")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Documentation generated at {output_path}")


if __name__ == "__main__":
    generate_docs("REFERENCE.md")
