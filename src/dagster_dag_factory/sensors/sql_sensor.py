from typing import List, Any, Tuple, Optional, Dict
from dagster_dag_factory.sensors.base_sensor import BaseSensor, SensorRegistry
from dagster_dag_factory.configs.database import DatabaseConfig


class SqlItem:
    """
    Helper class to represent the discovered state (High Water Mark).
    """
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata = metadata

    @property
    def key(self) -> str:
        """Unique key for the run (High Water Mark value)."""
        return str(self.metadata.get("cursor", "unknown"))

    @property
    def modified_ts(self) -> float:
        """Timestamp helper."""
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Metadata for Jinja context."""
        return self.metadata


@SensorRegistry.register("SQLSERVER")
@SensorRegistry.register("POSTGRES")
@SensorRegistry.register("SNOWFLAKE")
class SqlSensor(BaseSensor):
    """
    Sensor for SQL databases using High Water Mark (HWM) tracking.
    Polls for the MAX() value of a column and triggers a run if it has increased.
    """
    source_config_schema = DatabaseConfig

    def check(
        self, 
        context: Any, 
        source_config: DatabaseConfig, 
        resource: Any, 
        cursor: Optional[str] = None,
        **kwargs
    ) -> Tuple[List[SqlItem], Optional[str]]:
        """
        Executes discovery query based on the data-driven marker pattern.
        Moves cursor to the highest marker value actually found in the results.
        """
        # 1. Query for the new marker (MAX) and count within the window
        # Optimized: Only looks for records strictly greater than last_cursor
        # We strip trailing semicolons to prevent syntax errors in subqueries
        base_sql = source_config.sql.strip().rstrip(';')
        
        # Use native high-precision datetime for parameterization.
        # This is the ONLY bulletproof way to avoid precision loss (> vs >= issues)
        # and conversion errors (241) in SQL Server.
        import pendulum
        from datetime import datetime
        try:
            p_dt = pendulum.parse(cursor) if cursor else pendulum.datetime(1970, 1, 1)
            # Create a native naive datetime with microsecond precision
            params_dt = datetime(
                p_dt.year, p_dt.month, p_dt.day, p_dt.hour, p_dt.minute, p_dt.second, p_dt.microsecond
            )
        except:
            params_dt = datetime(1970, 1, 1)

        sql = f"""
            SELECT 
                MAX(discovery_query.{source_config.cursor_column}) as new_marker,
                COUNT(*) as record_count
            FROM ({base_sql}) as discovery_query
            WHERE discovery_query.{source_config.cursor_column} > ?
        """
        
        # Note: If the user provided a complex query in 'sql', we wrap it.
        # But for simplicity in the demo, we assume they provide a SELECT.
        rows = resource.execute_query(sql, params=(params_dt,))
        
        if not rows or not rows[0]:
            return [], cursor

        row = rows[0]
        new_marker = row.get("new_marker")
        count = row.get("record_count", 0)
        
        if count > 0 and new_marker:
            # Format to exactly 3 digits for SQL Server DATETIME compatibility
            new_cursor_str = new_marker.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            # Standardize last_cursor as well
            last_cursor_str = "1970-01-01 00:00:00.000"
            if cursor:
                try:
                    last_cursor_str = pendulum.parse(cursor).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                except:
                    last_cursor_str = cursor

            # We return a single item representing the time window
            item = SqlItem({
                "cursor": new_cursor_str,
                "last_cursor": last_cursor_str,
                "record_count": count
            })
            
            return [item], new_cursor_str

        return [], cursor
