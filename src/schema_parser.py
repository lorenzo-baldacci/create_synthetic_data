import json
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, StringType,
)

_TYPE_MAP = {
    "INT": IntegerType(),
    "INTEGER": IntegerType(),
    "BIGINT": LongType(),
    "LONG": LongType(),
    "FLOAT": FloatType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
    "BOOL": BooleanType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "STRING": StringType(),
    "VARCHAR": StringType(),
    "TEXT": StringType(),
}


def parse_schema(schema_file: str):
    """Parse a JSON schema file and return (StructType, [(name, sql_type_str), ...])."""
    with open(schema_file) as f:
        data = json.load(f)

    columns = data.get("columns", [])
    if not columns:
        raise ValueError(f"No columns found in schema file: {schema_file}")

    fields = []
    col_tuples = []
    for col in columns:
        name = col["name"]
        sql_type = col["type"].upper()
        spark_type = _TYPE_MAP.get(sql_type, StringType())
        fields.append(StructField(name, spark_type, nullable=True))
        col_tuples.append((name, sql_type))

    return StructType(fields), col_tuples
