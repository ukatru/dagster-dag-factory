import click
import os
import sys
import yaml
from pathlib import Path
from typing import Any, Dict, Optional

from dagster_dag_factory.factory.dagster_factory import DagsterFactory
from dagster_dag_factory.factory.registry import OperatorRegistry
from dagster_dag_factory.factory.utils.logging import log_header, log_action

@click.group()
def cli():
    """Dagster DAG Factory CLI - Lint, Describe, and Troubleshoot pipelines."""
    pass

@cli.command()
@click.option("--path", "-p", required=True, type=click.Path(exists=True), help="Path to the pipelines directory.")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed build logs.")
def lint(path, verbose):
    """Lint all YAML pipelines in a directory."""
    base_path = Path(path)
    # If the path points to definitions.py or similar, move up to find the root
    if base_path.suffix == ".py":
        base_path = base_path.parent
    
    # Check if we are pointing to the pipelines root (which contains 'defs')
    if not (base_path / "defs").exists() and (base_path.parent / "defs").exists():
        base_path = base_path.parent

    click.echo(f"Linting pipelines in: {base_path}")
    
    try:
        # Initialize factory in verbose mode if requested
        factory = DagsterFactory(base_dir=base_path, verbose_build=verbose or True)
        factory.build_definitions()
        click.secho("\n✅ All pipelines linted successfully!", fg="green", bold=True)
    except Exception as e:
        click.secho(f"\n❌ Linting failed: {e}", fg="red", bold=True)
        sys.exit(1)

@cli.command()
def list_operators():
    """List all registered operators."""
    click.echo("Registered Operators:")
    click.echo("-" * 60)
    for (source, target), op_class in OperatorRegistry._registry.items():
        click.echo(f"{source.ljust(15)} -> {target.ljust(15)} | {op_class.__name__}")

@cli.command()
@click.option("--file", "-f", required=True, type=click.Path(exists=True), help="Path to the YAML pipeline file.")
def inspect(file):
    """Inspect how a YAML translates into Dagster Assets (Dry-Run)."""
    yaml_path = Path(file)
    click.echo(f"Inspecting pipeline: {yaml_path.name}")
    click.echo("-" * 60)

    try:
        with open(yaml_path) as f:
            config = yaml.safe_load(f) or {}

        if "assets" not in config:
            click.secho("No 'assets' found in YAML.", fg="yellow")
            return

        for asset_conf in config["assets"]:
            name = asset_conf.get("name")
            click.secho(f"\nAsset: {name}", fg="green", bold=True)
            click.echo(f"Group:       {asset_conf.get('group', 'default')}")
            click.echo(f"Description: {asset_conf.get('description', 'N/A')}")
            
            source = asset_conf.get("source", {})
            target = asset_conf.get("target", {})
            
            click.echo(f"Source:      {source.get('type')} ({source.get('connection')})")
            click.echo(f"Target:      {target.get('type')} ({target.get('connection')})")
            
            # Show raw configs (as seen by the factory at build-time)
            click.secho("\n[Raw Source Payload]", fg="cyan")
            click.echo(yaml.dump(source.get("configs", {}), default_flow_style=False))
            
            click.secho("[Raw Target Payload]", fg="cyan")
            click.echo(yaml.dump(target.get("configs", {}), default_flow_style=False))
            
    except Exception as e:
        click.secho(f"Error inspecting file: {e}", fg="red")
        sys.exit(1)

@cli.command()
@click.argument("args", nargs=-1)
def describe(args):
    """Describe the configuration schema for an operator (2 args: SOURCE TARGET) or a resource (1 arg: TYPE)."""
    if len(args) == 2:
        source_type, target_type = args
        op_class = OperatorRegistry.get_operator(source_type, target_type)
        if not op_class:
            click.secho(f"No operator found for {source_type} -> {target_type}", fg="yellow")
            return

        click.secho(f"Operator: {op_class.__name__}", bold=True)
        click.echo("=" * 40)
        
        # Source Config
        click.secho("\n[Source Config]", fg="cyan", bold=True)
        if op_class.source_config_schema:
            _print_schema(op_class.source_config_schema)
        else:
            click.echo("No schema defined (Loose Dictionaries)")

        # Target Config
        click.secho("\n[Target Config]", fg="cyan", bold=True)
        if op_class.target_config_schema:
            _print_schema(op_class.target_config_schema)
        else:
            click.echo("No schema defined (Loose Dictionaries)")
            
    elif len(args) == 1:
        res_type = args[0]
        import dagster_dag_factory.resources as resources_module
        
        # Look for the resource class in the resources module
        # Note: res_type from user might be lowercase, but registry/module might use specific casing
        # We try to find a case-insensitive match (e.g., 's3' -> 'S3Resource')
        res_class = None
        for name in dir(resources_module):
            if name.upper() == res_type.upper() or name.upper() == f"{res_type.upper()}RESOURCE":
                res_class = getattr(resources_module, name)
                break
        
        if not res_class:
            click.secho(f"No resource type found for '{res_type}'", fg="yellow")
            return
            
        click.secho(f"Resource: {res_class.__name__}", bold=True)
        click.echo("=" * 40)
        
        # Print schema for the ConfigurableResource
        # ConfigurableResource is a pydantic model, but it might wrap its fields in a specific way
        _print_schema(res_class)
    else:
        click.secho("Usage: dag-factory describe <TYPE>  -or-  dag-factory describe <SOURCE> <TARGET>", fg="red")

def _print_schema(model):
    """Helper to print Pydantic model fields in a human-readable way."""
    from pydantic import ConfigDict
    
    # Use model_json_schema() for Pydantic V2
    schema = model.model_json_schema()
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    for field, info in properties.items():
        is_req = "*" if field in required else " "
        f_type = info.get("type", "any")
        desc = info.get("description", "")
        click.echo(f"{is_req} {field.ljust(20)} | {f_type.ljust(10)} | {desc}")

if __name__ == "__main__":
    cli()
