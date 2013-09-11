"""
Module nfldb provides command line tools and a library for maintaining
and querying a relational database of play-by-play NFL data. The data
is imported from [nflgame](https://github.com/BurntSushi/nflgame),
which in turn gets its data from a JSON feed on NFL.com's live
GameCenter pages. This data includes, but is not limited to, game
schedules, scores, rosters and play-by-play data for every preseason,
regular season and postseason game dating back to 2009.

In theory, both `nfldb` and `nflgame` provide access to the same data.
The difference is in the execution. In order to search data in nflgame,
a large JSON file needs to be read from disk and loaded into Python
data structures for each game. Conversely, nfldb's data is stored in
a relational database, which can be searched and retrieved faster
than nflgame by a few orders of magnitude. Moreover, the relational
organization of data in nfldb allows for a convenient
[query interface](http://goo.gl/Sd6MN2) to search NFL play data.

The database can be updated with real time data from active games by
running the `nfldb-update` script included with this module as often as
you like. Roster updates are done automatically at a minimum interval
of 12 hours.

nfldb has [comprehensive API documentation](http://pdoc.burntsushi.net/nfldb)
and a [wiki with examples](https://github.com/BurntSushi/nfldb/wiki).

nfldb can be used in conjunction with
[nflvid](https://pypi.python.org/pypi/nflvid)
to
[search and watch NFL game footage](http://goo.gl/1qSwJw).
"""
from __future__ import absolute_import, division, print_function

from nfldb.db import __pdoc__ as __db_pdoc__
from nfldb.db import api_version, connect, now, set_timezone, schema_version
from nfldb.db import Tx
from nfldb.query import __pdoc__ as __query_pdoc__
from nfldb.query import aggregate, current, Query, QueryOR
from nfldb.team import standard_team
from nfldb.types import __pdoc__ as __types_pdoc__
from nfldb.types import stat_categories
from nfldb.types import Category, Clock, Enums, Drive, FieldPosition, Game
from nfldb.types import Play, Player, PlayPlayer, PossessionTime, Team
from nfldb.version import __pdoc__ as __version_pdoc__
from nfldb.version import __version__

__pdoc__ = __db_pdoc__
__pdoc__ = dict(__pdoc__, **__query_pdoc__)
__pdoc__ = dict(__pdoc__, **__types_pdoc__)
__pdoc__ = dict(__pdoc__, **__version_pdoc__)


# Export selected identifiers from sub-modules.
__all__ = [
    # nfldb.db
    'api_version', 'connect', 'now', 'set_timezone', 'schema_version',
    'Tx',

    # nfldb.query
    'aggregate', 'current', 'Query', 'QueryOR',

    # nfldb.team
    'standard_team',

    # nfldb.types
    'stat_categories',
    'Category', 'Clock', 'Enums', 'Drive', 'FieldPosition', 'Game',
    'Play', 'Player', 'PlayPlayer', 'PossessionTime', 'Team',

    # nfldb.version
    '__version__',
]
