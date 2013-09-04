from __future__ import absolute_import, division, print_function
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
import re

from psycopg2.extensions import cursor as tuple_cursor

from nfldb.db import Tx
import nfldb.types as types

try:
    strtype = basestring
except NameError:  # I have lofty hopes for Python 3.
    strtype = str


__pdoc__ = {}


def _select_fields(prefix, fields):
    return ', '.join('%s.%s' % (prefix, field) for field in fields)


def aggregate(objs):
    """
    Given any collection of Python objects that provide a
    `play_players` attribute, `aggregate` will return a list of
    `PlayPlayer` objects with statistics aggregated over each player.
    (As a special case, if an element in `objs` is itself a
    `nfldb.PlayPlayer` object, then it is used and a `play_players`
    attribute is not rquired.)

    For example, `objs` could be a mixed list of `nfldb.Game` and
    `nfldb.Play` objects.

    The order of the list returned is stable with respect to the
    order of players obtained from each element in `objs`.
    """
    summed = OrderedDict()
    for obj in objs:
        pps = [obj] if isinstance(obj, types.PlayPlayer) else obj.play_players
        for pp in pps:
            if pp.player_id not in summed:
                summed[pp.player_id] = pp._copy()
            else:
                summed[pp.player_id]._add(pp)
    return summed.values()
    """
    Adds `nfldb.Condition` objects to the condition list `conds` for
    the `table`. Only the values in `kwargs` that correspond to keys in
    `keys` are used.
    """
    def sanitize(s):
        return re.sub('__(eq|ne|gt|lt|ge|le)$', '', s)
    for k, v in ((k, v) for k, v in kwargs.items() if sanitize(k) in keys):
        conds.append(Comparison(table, k, v))


def _cond_where_sql(cursor, conds, tables):
    """
    Returns a list of valid SQL comparisons derived from a list of
    `nfldb.Condition` objects in `conds` and restricted to the list
    of table names `tables`.
    """
    isa = isinstance
    pieces = []
    for c in conds:
        if isa(c, Query) or (isa(c, Comparison) and c.table in tables):
            sql = c._sql_where(cursor, tables)
            if len(sql) > 0:
                pieces.append(sql)
    return pieces


def _where_and(*exprs):
    """
    Given a list of SQL expressions, return a valid `WHERE` clause for
    a SQL query with the exprs AND'd together.

    Exprs that are empty are omitted.
    """
    anded = ' AND '.join('(%s)' % expr for expr in exprs if expr)
    return _prefix_or_empty('WHERE ', anded)


def _prefix_or_empty(prefix, s):
    if not s or len(s) == 0:
        return ''
    return '%s%s' % (prefix, s)


def _sql_pkey_in(cur, pkeys, ids, prefix=''):
    pkeys = ['%s%s' % (prefix, pk) for pk in pkeys]
    if len(ids) == 0:
        nulls = ', '.join(['NULL'] * len(pkeys))
        return '(%s) IN ((%s))' % (', '.join(pkeys), nulls)

    return '(%s) IN %s' % (', '.join(pkeys), cur.mogrify('%s', (tuple(ids),)))


class Condition (object):
    """
    An abstract class that describes the interface of components
    in a SQL query.
    """
    def __init__(self):
        assert False, "Condition class cannot be instantiated."

    def _tables(self):
        """Returns a `set` of tables used in this condition."""
        assert False, "subclass responsibility"

    def _sql_where(self, cursor, table):
        """
        Returns an escaped SQL string that can be safely substituted
        into the WHERE clause of a SELECT query for a particular
        `table`.
        """
        assert False, "subclass responsibility"


class Comparison (Condition):
    """
    A representation of a single comparison in a `nfldb.Query`.

    This corresponds to a field name, a value and one of the following
    operators: `=`, `!=`, `<`, `<=`, `>` or `>=`.
    """
    def __init__(self, table, kw, value):
        """
        Introduces a new condition given a user specified keyword `kw`
        with a `table` and a user provided value. The operator to be
        used is inferred from the suffix of `kw`. If `kw` has no suffix
        or a `__eq` suffix, then `=` is used. A suffix of `__ge` means
        `>=` is used, `__lt` means `<`, and so on.

        If `value` is of the form `sql(...)` then the value represented
        by `...` is written to the SQL query without escaping.
        """
        self.operator = '='
        """The operator used in this condition."""

        self.table = table
        """The table for this column. It may be empty."""

        self.column = None
        """The SQL column name in this condition."""

        self.value = value
        """The Python value to compare the SQL column to."""

        suffixes = {
            '__eq': '=', '__ne': '!=',
            '__lt': '<', '__le': '<=', '__gt': '>', '__ge': '>=',
        }
        for suffix, op in suffixes.items():
            if kw.endswith(suffix):
                self.operator = op
                self.column = kw[0:-4]
        if self.column is None:
            self.column = kw

    def _tables(self):
        return set([self.table])

    def _sql_where(self, cursor, _):
        prefix = '%s.' % self.table
        paramed = '%s%s %s %s' % (prefix, self.column, self.operator, '%s')
        if isinstance(self.value, strtype) and self.value.startswith('sql('):
            return paramed % self.value[4:-1]
        else:
            return cursor.mogrify(paramed, (self.value,))


class Query (Condition):
    """
    A query represents a set of criteria to search a database.
    """

    def __init__(self, db, orelse=False):
        self._db = db
        """A psycopg2 database connection object."""

        self._andalso = []
        """A list of conjunctive conditions."""

        self._orelse = []
        """
        A list of disjunctive conditions applied to
        `Query._andalso`.
        """

        self._default_cond = self._orelse if orelse else self._andalso
        """
        Whether to use conjunctive or disjunctive conditions by
        default.
        """

    def andalso(self, *conds):
        """
        Adds the list of `nfldb.Query` objects in `conds` to this
        query's list of conjunctive conditions.
        """
        self._andalso += conds
        return self

    def orelse(self, *conds):
        """
        Adds the list of `nfldb.Query` objects in `conds` to this
        query's list of disjunctive conditions. Note that a disjunction
        on this query applies to the entire set of condition in this
        query's list of conjunctive conditions.
        """
        self._orelse += conds
        return self

    def games(self, **kw):
        """
        Specify search criteria for an NFL game. The possible fields
        correspond to columns in the SQL table. They are documented
        as instance variables in the `nfldb.Game` class. In addition,
        there are some special fields that provide convenient access
        to common conditions:

          * **team** - Find games that the team given played in, regardless
                       of whether it is the home or away team.
          * **winner** - Find games where the winner is the team given.
          * **loser** - Find games where the loser is the team given.
        """
        _append_conds(self._default_cond, 'game', types.Game._sql_fields, kw)
        if 'team' in kw:
            ors = {'home_team': kw['team'], 'away_team': kw['team']}
            self.andalso(Query(self._db, orelse=True).games(**ors))
        if 'winner' in kw:
            self.games(finished=True)
            q1, q2 = Query(self._db), Query(self._db)
            q1.games(home_team=kw['winner'], home_score__gt='sql(away_score)')
            q2.games(away_team=kw['winner'], away_score__gt='sql(home_score)')
            self.andalso(q1.orelse(q2))
        if 'loser' in kw:
            self.games(finished=True)
            q1, q2 = Query(self._db), Query(self._db)
            q1.games(home_team=kw['loser'], home_score__lt='sql(away_score)')
            q2.games(away_team=kw['loser'], away_score__lt='sql(home_score)')
            self.andalso(q1.orelse(q2))
        return self

    def drives(self, **kw):
        _append_conds(self._default_cond, 'drive', types.Drive._sql_fields, kw)
        return self

    def plays(self, **kw):
        _append_conds(self._default_cond, 'play', types.Play._sql_fields, kw)
        _append_conds(self._default_cond, 'play_player',
                      types.PlayPlayer._sql_fields, kw)
        return self

    def players(self, **kw):
        _append_conds(self._default_cond, 'player',
                      types.Player._sql_fields, kw)
        return self

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        ids = self._ids('game')
        results = []
        q = 'SELECT %s FROM game %s'
        with Tx(self._db) as cursor:
            q = q % (
                _select_fields('game', types.Game._sql_fields),
                _where_and(_sql_pkey_in(cursor, ['gsis_id'], ids['game'])),
            )
            cursor.execute(q)

            for row in cursor.fetchall():
                results.append(types.Game.from_row(self._db, row))
        return results

    def as_drives(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Drive` objects.
        """
        ids = self._ids('drive')
        results = []
        q = 'SELECT %s FROM drive %s'
        with Tx(self._db) as cursor:
            pkey = _sql_pkey_in(cursor, ['gsis_id', 'drive_id'], ids['drive'])
            if pkey is None:
                pkey = _sql_pkey_in(cursor, ['gsis_id'], ids['game'])
            q = q % (
                _select_fields('drive', types.Drive._sql_fields),
                _where_and(pkey),
            )
            cursor.execute(q)

            for row in cursor.fetchall():
                results.append(types.Drive.from_row(self._db, row))
        return results

    def as_plays(self):
        """
        Executes the query and returns the results as a list of
        `nlfdb.Play` objects.
        """
        # Exclude common columns between the `play` and `play_player`
        # tables.
        play_fields = _select_fields('play', types.Play._sql_fields)
        player_fields = _select_fields('play_player',
                                       types.PlayPlayer._sql_fields)

        plays = OrderedDict()
        ids = self._ids('play')
        playids = set(ids['play'])

        with Tx(self._db, factory=tuple_cursor) as cursor:
            pkey = None
            if len(ids['drive']) < 8000:
                pkey = _sql_pkey_in(cursor, ['gsis_id', 'drive_id'],
                                    ids['drive'])
            if pkey is None:
                pkey = _sql_pkey_in(cursor, ['gsis_id'], ids['game'])

            cursor.execute('''
                SELECT %s FROM play %s
                ORDER BY gsis_id, drive_id, play_id
            ''' % (play_fields, _where_and(pkey)))
            init = types.Play._from_tuple
            for t in cursor.fetchall():
                pid = (t[0], t[1], t[2])
                if pid in playids:
                    p = init(self._db, t)
                    p._play_players = []  # Load this below.
                    plays[pid] = p

        with Tx(self._db, factory=tuple_cursor) as cursor:
            cursor.execute('''
                SELECT %s FROM play_player %s
            ''' % (player_fields, _where_and(pkey)))
            init = types.PlayPlayer._from_tuple
            for t in cursor.fetchall():
                pid = (t[0], t[1], t[2])
                if pid in playids:
                    pp = init(self._db, t)
                    plays[pid]._play_players.append(pp)
        return plays.values()

    def as_players(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Player` objects.
        """
        ids = self._ids('player')
        results = []
        q = 'SELECT %s FROM player %s'
        with Tx(self._db) as cursor:
            q = q % (
                _select_fields('player', types.Player._sql_fields),
                _where_and(_sql_pkey_in(cursor, ['player_id'], ids['player'])),
            )
            cursor.execute(q)

            for row in cursor.fetchall():
                results.append(types.Player.from_row(self._db, row))
        return results

    def _has_table(self, _):
        return True

    def _tables(self):
        tabs = set()
        for cond in self._andalso + self._orelse:
            tabs = tabs.union(cond._tables())
        return tabs

    def _sql_where(self, cur, tables):
        disjunctions = []
        andsql = ' AND '.join(_cond_where_sql(cur, self._andalso, tables))
        if len(andsql) > 0:
            andsql = '(%s)' % andsql
            disjunctions.append(andsql)
        disjunctions += _cond_where_sql(cur, self._orelse, tables)

        if len(disjunctions) == 0:
            return ''
        return '(%s)' % (' OR '.join(disjunctions))

    def _ids(self, as_table):
        """
        Returns a dictionary of primary keys matching the criteria
        specified in this query for the following tables: game, drive,
        play and player. The returned dictionary will have a key for
        each table with a corresponding tuple, which may be empty.

        Each tuple contains primary key values for that table. In the
        case of the `drive` and `play` table, those values are tuples.
        """
        # Initialize sets to `None`. This distinguishes an empty result
        # set and a lack of search.
        game, drive, play, player = [None] * 4

        with Tx(self._db, factory=tuple_cursor) as cur:
            def pkin(pkeys, ids, prefix=''):
                return _sql_pkey_in(cur, pkeys, ids, prefix=prefix)

            # Look at what tables are being queried and fetch identifiers
            # based on it. Always assume that the table containing data we're
            # ultimately gathering is included.
            tables = self._tables()
            tables.add(as_table)

            # Start with games since it has the smallest space.
            if 'game' in tables:
                game = set()
                cur.execute(
                    'SELECT gsis_id FROM game %s'
                    % _where_and(self._sql_where(cur, ['game'])))
                for row in cur.fetchall():
                    game.add(row[0])

            # Filter by drive...
            if 'drive' in tables:
                idexp = None
                if game is not None:
                    idexp = pkin(['gsis_id'], game)

                cur.execute('''
                    SELECT gsis_id, drive_id
                    FROM drive
                    %s
                ''' % (_where_and(idexp, self._sql_where(cur, ['drive']))))

                game, drive = set(), set()
                for row in cur.fetchall():
                    game.add(row[0])
                    drive.add((row[0], row[1]))

            # Filter by play.
            # This is a little messed, since we're searching on the `play`
            # and `play_player` tables.
            if 'play' in tables:
                ide = None
                if drive is not None:
                    ide = pkin(['gsis_id', 'drive_id'], drive)
                elif game is not None:
                    ide = pkin(['gsis_id'], game)

                # When there are no criteria for `play_player`, then we've
                # got to make sure to always hit the `play` table.
                # Otherwise, we only hit the `play` table when it's in the
                # criteria. This is to allow for pathological `as_plays` calls
                # with no criteria to work as expected.
                if 'play_player' not in tables or 'play' in self._tables():
                    q = '''
                        SELECT play.gsis_id, play.drive_id, play.play_id
                        FROM play %s
                    ''' % (_where_and(ide, self._sql_where(cur, ['play'])))
                    cur.execute(q)

                    game, drive, play = set(), set(), set()
                    for row in cur.fetchall():
                        game.add(row[0])
                        drive.add((row[0], row[1]))
                        play.add((row[0], row[1], row[2]))

                if 'play_player' in tables:
                    # The trick here is to take the intersection of the results
                    # from play_player with the results from play.
                    # But we have to be careful: if there were no prior
                    # criteria specified to game/drive/play, then the
                    # intersection would erroneously return the empty set.
                    where = self._sql_where(cur, ['play_player'])
                    q = '''
                        SELECT gsis_id, drive_id, play_id
                        FROM play_player %s
                    ''' % (_where_and(ide, where))
                    cur.execute(q)
                    pp_game, pp_drive, pp_play = set(), set(), set()
                    for row in cur.fetchall():
                        pp_game.add(row[0])
                        pp_drive.add((row[0], row[1]))
                        pp_play.add((row[0], row[1], row[2]))
                    game = (game or pp_game).intersection(pp_game)
                    drive = (drive or pp_drive).intersection(pp_drive)
                    play = (play or pp_play).intersection(pp_play)

            # Finally filter by player.
            if 'player' in tables:
                # Cut down the game/drive/play ids to only what the players
                # participated in.
                idexp = None
                old_playids = None if play is None else play.copy()
                old_drvids = None if drive is None else drive.copy()

                if drive is not None and len(drive) < 5000:
                    idexp = pkin(['gsis_id', 'drive_id'], drive)
                elif game is not None:
                    idexp = pkin(['gsis_id'], game)

                cur.execute('''
                    SELECT player_id FROM player %s
                ''' % (_where_and(self._sql_where(cur, ['player']))))
                player = set()
                for row in cur.fetchall():
                    player.add(row[0])

                cur.execute('''
                    SELECT gsis_id, drive_id, play_id, player_id
                    FROM play_player %s
                ''' % (_where_and(idexp, pkin(['player_id'], player))))

                game, drive, play, player = set(), set(), set(), set()
                for row in cur.fetchall():
                    pid = (row[0], row[1], row[2])
                    if old_playids is not None and pid not in old_playids:
                        continue

                    drvid = (row[0], row[1])
                    if old_drvids is not None and drvid not in old_drvids:
                        continue

                    game.add(row[0])
                    drive.add(drvid)
                    play.add(pid)
                    player.add(row[3])

        return {
            'game': tuple(game or []), 'drive': tuple(drive or []),
            'play': tuple(play or []), 'player': tuple(player or []),
        }
