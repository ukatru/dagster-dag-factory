from dagster import EnvVar


class EnvVarAccessor:
    """
    Helper to provide access to Dagster EnvVar objects via dot or item notation.
    {{ env.VAR_NAME }} -> EnvVar("VAR_NAME")
    """

    def __getattr__(self, name: str) -> EnvVar:
        return EnvVar(name)

    def __getitem__(self, name: str) -> EnvVar:
        return EnvVar(name)

    def get_raw(self, name: str) -> str:
        """Helper for string interpolation where we need the actual env value."""
        import os

        return os.environ.get(name, f"{{{{env.{name}}}}}")
