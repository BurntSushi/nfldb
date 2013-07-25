from nfldb.db import api_version, connect, version

api_version = api_version  # Doco hack for epydoc.
"""
The schema version that this library corresponds to.
When the schema version of the database is less than this value,
connect will automatically update the schema to the latest
version before doing anything else.
"""

# Export selected identifiers from sub-modules.
__all__ = [
    'api_version', 'connect', 'version',  # nfldb.db
]
