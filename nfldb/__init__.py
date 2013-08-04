from nfldb.db import api_version, connect, version
from nfldb.db import Tx

api_version = api_version  # Doco hack for epydoc.
"""
The schema version that this library corresponds to.
When the schema version of the database is less than this value,
connect will automatically update the schema to the latest
version before doing anything else.
"""

enums = enums  # Doco hack for epydoc.
"""
Enums is a dictionary that contains all possible values for each
enum type in the database, represented as lists. The ordering
of each list is the same as the ordering in the database.
"""

# Export selected identifiers from sub-modules.
__all__ = [
    'api_version', 'connect', 'version',  # nfldb.db
    'Tx',
]
