import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied


def validate_catalog(ws: WorkspaceClient, catalog_name: str) -> None:
    """Verify the catalog exists and is accessible. Exits on failure."""
    try:
        ws.catalogs.get(catalog_name)
        print(f"Catalog '{catalog_name}' found.")
    except NotFound:
        print(f"Error: Catalog '{catalog_name}' does not exist.", file=sys.stderr)
        sys.exit(1)
    except PermissionDenied:
        print(f"Error: Permission denied accessing catalog '{catalog_name}'.", file=sys.stderr)
        sys.exit(1)


def ensure_schema(ws: WorkspaceClient, catalog_name: str, schema_name: str) -> None:
    """Ensure the schema exists, creating it if necessary. Exits on permission failure."""
    full_name = f"{catalog_name}.{schema_name}"
    try:
        ws.schemas.get(full_name)
        print(f"Schema '{full_name}' already exists, skipping creation.")
    except NotFound:
        print(f"Schema '{full_name}' not found. Creating...")
        try:
            ws.schemas.create(name=schema_name, catalog_name=catalog_name)
            print(f"Schema '{full_name}' created successfully.")
        except PermissionDenied:
            print(
                f"Error: Permission denied creating schema '{full_name}'.",
                file=sys.stderr,
            )
            sys.exit(1)
    except PermissionDenied:
        print(
            f"Error: Permission denied accessing schema '{full_name}'.",
            file=sys.stderr,
        )
        sys.exit(1)
