"""
Module nfldb provides command line tools and a library for maintaining
and querying a relational database of play-by-play NFL data.
"""
from __future__ import absolute_import, division, print_function

from nfldb.db import __pdoc__ as __db_pdoc__
from nfldb.db import api_version, connect, now, set_timezone, schema_version
from nfldb.db import Tx
from nfldb.query import __pdoc__ as __query_pdoc__
from nfldb.query import aggregate, Query
from nfldb.types import __pdoc__ as __types_pdoc__
from nfldb.types import stat_categories
from nfldb.types import Category, Clock, Enums, Drive, FieldPosition, Game
from nfldb.types import Play, Player, PlayPlayer, PossessionTime, Team
from nfldb.version import __version__

__pdoc__ = __db_pdoc__
__pdoc__ = dict(__pdoc__, **__query_pdoc__)
__pdoc__ = dict(__pdoc__, **__types_pdoc__)


# Export selected identifiers from sub-modules.
__all__ = [
    # nfldb.db
    'api_version', 'connect', 'now', 'set_timezone', 'schema_version',
    'Tx',

    # nfldb.query
    'aggregate', 'Query',

    # nfldb.types
    'stat_categories',
    'Category', 'Clock', 'Enums', 'Drive', 'FieldPosition', 'Game',
    'Play', 'Player', 'PlayPlayer', 'PossessionTime', 'Team',

    # nfldb.version
    '__version__',
]
