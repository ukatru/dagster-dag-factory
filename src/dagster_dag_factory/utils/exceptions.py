from typing import Optional


class DagsterFactoryError(Exception):
    """
    Base exception for errors occurring during the Dagster build phase.
    
    Provides context about the file and asset that caused the error.
    """

    def __init__(
        self,
        message: str,
        file_name: Optional[str] = None,
        asset_name: Optional[str] = None,
        error_type: str = "VALIDATION_ERROR",
    ):
        self.message = message
        self.file_name = file_name
        self.asset_name = asset_name
        self.error_type = error_type
        super().__init__(self.message)

    def __str__(self) -> str:
        ctx = []
        if self.file_name:
            ctx.append(f"file: {self.file_name}")
        if self.asset_name:
            ctx.append(f"asset: {self.asset_name}")
        
        context_str = f" [{', '.join(ctx)}]" if ctx else ""
        return f"{self.error_type}{context_str}: {self.message}"
