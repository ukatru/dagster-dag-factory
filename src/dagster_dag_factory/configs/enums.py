from enum import Enum

class CompressionType(str, Enum):
    ZIP = 'ZIPFILE'
    GUNZIP = 'GUNZIP'

class CompressionAction(str, Enum):
    COMPRESS = 'COMPRESS'
    DECOMPRESS = 'DECOMPRESS'

class CsvQuoting(str, Enum):
    NONE = 'NONE'
    ALL = 'ALL'
    MINIMAL = 'MINIMAL'
    NONNUMERIC = 'NONNUMERIC'

class S3Mode(str, Enum):
    COPY = 'COPY'
    MULTI_FILE = 'MULTI_FILE'

class S3ObjectType(str, Enum):
    CSV = 'CSV'
    JSON = 'JSON'
    PARQUET = 'PARQUET'
