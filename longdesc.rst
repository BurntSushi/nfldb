Module nfldb provides command line tools and a library for maintaining
and querying a relational database of play-by-play NFL data. The data is
imported from `nflgame <https://github.com/BurntSushi/nflgame>`__, which
in turn gets its data from a JSON feed on NFL.com's live GameCenter
pages. This data includes, but is not limited to, game schedules,
scores, rosters and play-by-play data for every preseason, regular
season and postseason game dating back to 2009.

Here is a small teaser that shows how to use nfldb to find the top five
passers in the 2012 regular season:

::

    #!python
    import nfldb

    db = nfldb.connect()
    q = nfldb.Query(db)

    q.game(season_year=2012, season_type='Regular')
    for pp in q.sort('passing_yds').limit(5).as_aggregate():
        print pp.player, pp.passing_yds

And the output is:

::

    [andrew@Liger ~] python2 top-five.py
    Drew Brees (NO, QB) 5177
    Matthew Stafford (DET, QB) 4965
    Tony Romo (DAL, QB) 4903
    Tom Brady (NE, QB) 4799
    Matt Ryan (ATL, QB) 4719

In theory, both ``nfldb`` and ``nflgame`` provide access to the same
data. The difference is in the execution. In order to search data in
nflgame, a large JSON file needs to be read from disk and loaded into
Python data structures for each game. Conversely, nfldb's data is stored
in a relational database, which can be searched and retrieved faster
than nflgame by a few orders of magnitude. Moreover, the relational
organization of data in nfldb allows for a convenient `query
interface <http://goo.gl/Sd6MN2>`__ to search NFL play data.

The database can be updated with real time data from active games by
running the ``nfldb-update`` script included with this module as often
as you're comfortable pinging NFL.com. (N.B. The JSON data itself only
updates every 15 seconds, so running ``nfldb-update`` faster than that
would be wasteful.) Roster updates are done automatically at a minimum
interval of 12 hours.

nfldb has `comprehensive API
documentation <http://pdoc.burntsushi.net/nfldb>`__ and a `wiki with
examples <https://github.com/BurntSushi/nfldb/wiki>`__.

nfldb can be used in conjunction with
`nflvid <https://pypi.python.org/pypi/nflvid>`__ to `search and watch
NFL game footage <http://goo.gl/Mckaf0>`__.

If you need help, please join us at our IRC channel ``#nflgame`` on
FreeNode.
