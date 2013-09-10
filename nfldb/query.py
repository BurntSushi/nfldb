from __future__ import absolute_import, division, print_function
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
import heapq
import re
import sys

from psycopg2.extensions import cursor as tuple_cursor

from nfldb.db import Tx
import nfldb.types as types

try:
    strtype = basestring
except NameError:  # I have lofty hopes for Python 3.
    strtype = str


__pdoc__ = {}


_sql_max_in = 4500
"""The maximum number of expressions to allow in a `IN` expression."""


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


def current(db):
    """
    Returns a triple of `nfldb.Enums.season_phase`, season year and week
    corresponding to values that `nfldb` thinks are current.

    Note that this only queries the database. Only the `nfldb-update`
    script fetches the current state from NFL.com.

    The values retrieved may be `None` if the season is over or if they
    haven't been updated yet by the `nfldb-update` script.
    """
    with Tx(db, factory=tuple_cursor) as cursor:
        cursor.execute('SELECT season_type, season_year, week FROM meta')
        return cursor.fetchone()
    return tuple([None] * 3)


def _append_conds(conds, tabtype, kwargs):
    """
    Adds `nfldb.Condition` objects to the condition list `conds` for
    the `table`. Only the values in `kwargs` that correspond to keys in
    `keys` are used.
    """
    keys = tabtype._sql_fields
    trim = _no_comp_suffix
    for k, v in ((k, v) for k, v in kwargs.items() if trim(k) in keys):
        conds.append(Comparison(tabtype, k, v))


def _no_comp_suffix(s):
    """Removes the comparison operator suffix from a search field."""
    return re.sub('__(eq|ne|gt|lt|ge|le)$', '', s)


def _comp_suffix(s):
    """
    Returns the comparison operator suffix given a search field.
    This does not include the `__` (double underscore).

    If no suffix is present, then `eq` is returned.
    """
    suffixes = ['eq', 'ne', 'lt', 'le', 'gt', 'ge']
    for suffix in suffixes:
        if s.endswith(suffix):
            return suffix
    return 'eq'


def _sql_where(cur, tables, andalso, orelse, prefix=None, aggregate=False):
    """
    Returns a valid SQL condition expression given a list of
    conjunctions and disjunctions. The list of disjunctions
    is given the lowest precedent via grouping with parentheses.
    """
    disjunctions = []
    andsql = _cond_where_sql(cur, andalso, tables, prefix=prefix,
                             aggregate=aggregate)
    andsql = ' AND '.join(andsql)

    if len(andsql) > 0:
        andsql = '(%s)' % andsql
        disjunctions.append(andsql)
    disjunctions += _cond_where_sql(cur, orelse, tables, prefix=prefix,
                                    aggregate=aggregate)

    if len(disjunctions) == 0:
        return ''
    return '(%s)' % (' OR '.join(disjunctions))


def _cond_where_sql(cursor, conds, tables, prefix=None, aggregate=False):
    """
    Returns a list of valid SQL comparisons derived from a list of
    `nfldb.Condition` objects in `conds` and restricted to the list
    of table names `tables`.
    """
    isa = isinstance
    pieces = []
    for c in conds:
        if isa(c, Query) or (isa(c, Comparison) and c._table in tables):
            sql = c._sql_where(cursor, tables, prefix=prefix,
                               aggregate=aggregate)
            if len(sql) > 0:
                pieces.append(sql)
    return pieces


def _prefix_and(*exprs, **kwargs):
    """
    Given a list of SQL expressions, return a valid `WHERE` clause for
    a SQL query with the exprs AND'd together.

    Exprs that are empty are omitted.

    A keyword argument `prefix` can be used to change the value of
    `WHERE ` to something else (e.g., `HAVING `).
    """
    anded = ' AND '.join('(%s)' % expr for expr in exprs if expr)
    if len(anded) == 0:
        return ''
    return kwargs.get('prefix', 'WHERE ') + anded


def _sql_pkey_in(cur, pkeys, ids, prefix=''):
    pkeys = ['%s%s' % (prefix, pk) for pk in pkeys]
    if ids.is_full:
        return None
    elif len(ids) == 0:
        nulls = ', '.join(['NULL'] * len(pkeys))
        return '(%s) IN ((%s))' % (', '.join(pkeys), nulls)

    return '(%s) IN %s' % (', '.join(pkeys), cur.mogrify('%s', (tuple(ids),)))


def _pk_play(cur, ids, tables=['game', 'drive']):
    pk = None
    if ('play' in tables or 'play_player' in tables) \
            and len(ids['play']) <= _sql_max_in:
        pk = _sql_pkey_in(cur, ['gsis_id', 'drive_id', 'play_id'], ids['play'])
    if 'drive' in tables and len(ids['drive']) <= _sql_max_in:
        pk = _sql_pkey_in(cur, ['gsis_id', 'drive_id'], ids['drive'])
    if 'game' in tables and pk is None:
        pk = _sql_pkey_in(cur, ['gsis_id'], ids['game'])
    return pk


def _play_set(ids):
    """
    Returns a value representing a set of plays in correspondence with
    the given `ids` dictionary. The value may be any combination
    of drive and play identifiers. Use `in_play_set` for membership
    testing.
    """
    if not ids['play'].is_full:
        return ('play', ids['play'])
    elif not ids['drive'].is_full:
        return ('drive', ids['drive'])
    else:
        return None


def _in_play_set(pset, play_pk):
    """
    Given a tuple `(gsis_id, drive_id, play_id)`, return `True`
    if and only if it exists in the play set `pset`.

    Valid values for `pset` can be constructed with `play_set`.
    """
    if pset is None:  # No criteria for drive/play. Always true, then!
        return True
    elif pset[0] == 'play':
        return play_pk in pset[1]
    elif pset[0] == 'drive':
        return play_pk[0:2] in pset[1]
    assert False, 'invalid play_set value'


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

    def _sql_where(self, cursor, table, prefix=None, aggregate=False):
        """
        Returns an escaped SQL string that can be safely substituted
        into the WHERE clause of a SELECT query for a particular
        `table`.

        The `prefix` parameter specifies a prefix to be used for each
        column written. If it's empty, then no prefix is used.

        If `aggregate` is `True`, then aggregate conditions should
        be used instead of regular conditions.
        """
        assert False, "subclass responsibility"


class Comparison (Condition):
    """
    A representation of a single comparison in a `nfldb.Query`.

    This corresponds to a field name, a value and one of the following
    operators: `=`, `!=`, `<`, `<=`, `>` or `>=`.
    """
    def __init__(self, tabtype, kw, value):
        """
        Introduces a new condition given a user specified keyword `kw`
        with a `tabtype` (e.g., `nfldb.Play`) and a user provided
        value. The operator to be used is inferred from the suffix of
        `kw`. If `kw` has no suffix or a `__eq` suffix, then `=` is
        used. A suffix of `__ge` means `>=` is used, `__lt` means `<`,
        and so on.

        If `value` is of the form `sql(...)` then the value represented
        by `...` is written to the SQL query without escaping.
        """
        self.operator = '='
        """The operator used in this condition."""

        self.tabtype = tabtype
        """The table type for this column."""

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

    @property
    def _table(self):
        return self.tabtype._table

    def _tables(self):
        return set([self.tabtype._table])

    def __str__(self):
        return '%s.%s %s %s' \
               % (self._table, self.column, self.operator, self.value)

    def _sql_where(self, cursor, tables, prefix=None, aggregate=False):
        field = self.tabtype._as_sql(self.column, prefix=prefix)
        if aggregate:
            field = 'SUM(%s)' % field
        paramed = '%s %s %s' % (field, self.operator, '%s')
        if isinstance(self.value, strtype) and self.value.startswith('sql('):
            return paramed % self.value[4:-1]
        else:
            if isinstance(self.value, tuple) or isinstance(self.value, list):
                paramed = paramed % 'ANY (%s)'
                self.value = list(self.value)  # Coerce tuples to pg ARRAYs...
            return cursor.mogrify(paramed, (self.value,))


def QueryOR(db):
    """An alias for `nfldb.Query(db, orelse=True)`."""
    return Query(db, orelse=True)


class Query (Condition):
    """
    A query represents a set of criteria to search a database.
    """

    def __init__(self, db, orelse=False):
        self._db = db
        """A psycopg2 database connection object."""

        self._sort_exprs = None
        """Expressions used to sort the results."""

        self._limit = None
        """The number of results to limit the search to."""

        self._sort_tables = []
        """The tables to restrain limiting criteria to."""

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

        # The aggregate counter-parts of the above.
        self._agg_andalso, self._agg_orelse = [], []
        if orelse:
            self._agg_default_cond = self._agg_orelse
        else:
            self._agg_default_cond = self._agg_andalso

    def sort(self, exprs):
        self._sort_exprs = exprs
        return self

    def limit(self, count):
        self._limit = count
        return self

    @property
    def _sorter(self):
        return Sorter(self._sort_exprs, self._limit,
                      restraining=self._sort_tables)

    def andalso(self, *conds):
        """
        Adds the list of `nfldb.Query` objects in `conds` to this
        query's list of conjunctive conditions.
        """
        self._andalso += conds
        return self

    def game(self, **kw):
        """
        Specify search criteria for an NFL game. The possible fields
        correspond to columns in the SQL table (or derived columns).
        They are documented as instance variables in the `nfldb.Game`
        class. In addition, there are some special fields that provide
        convenient access to common conditions:

          * **team** - Find games that the team given played in, regardless
                       of whether it is the home or away team.
        """
        _append_conds(self._default_cond, types.Game, kw)
        if 'team' in kw:
            ors = {'home_team': kw['team'], 'away_team': kw['team']}
            self.andalso(Query(self._db, orelse=True).game(**ors))
        return self

    def drive(self, **kw):
        _append_conds(self._default_cond, types.Drive, kw)
        return self

    def play(self, **kw):
        _append_conds(self._default_cond, types.Play, kw)
        _append_conds(self._default_cond, types.PlayPlayer, kw)

        # Technically, it isn't necessary to handle derived fields manually
        # since their SQL can be generated automatically, but it can be
        # much faster to express them in terms of boolean logic with other
        # fields rather than generate them.
        for field, value in kw.items():
            nosuff = _no_comp_suffix(field)
            suff = _comp_suffix(field)

            def replace_or(*fields):
                q = Query(self._db, orelse=True)
                ors = dict([('%s__%s' % (f, suff), value) for f in fields])
                self.andalso(q.play(**ors))

            if nosuff in types.PlayPlayer._derived_sums:
                replace_or(*types.PlayPlayer._derived_sums[nosuff])
        return self

    def player(self, **kw):
        _append_conds(self._default_cond, types.Player, kw)
        return self

    def aggregate(self, **kw):
        """
        This is just like `nfldb.Query.play`, except the search
        parameters are applied to aggregate statistics.

        Note that this method can **only** be used with
        `nfldb.Query.as_aggregate`. Use with any of the other `as_*`
        methods will result in an assertion error.
        """
        _append_conds(self._agg_default_cond, types.Play, kw)
        _append_conds(self._agg_default_cond, types.PlayPlayer, kw)
        return self

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        self._sort_tables = [types.Game]
        ids = self._ids('game', self._sorter)
        results = []
        q = 'SELECT %s FROM game %s %s'
        with Tx(self._db) as cursor:
            q = q % (
                types._select_fields(types.Game),
                _prefix_and(_sql_pkey_in(cursor, ['gsis_id'], ids['game'])),
                self._sorter.sql(tabtype=types.Game),
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
        self._sort_tables = [types.Drive]
        ids = self._ids('drive', self._sorter)
        tables = self._tables()
        results = []
        q = 'SELECT %s FROM drive %s %s'
        with Tx(self._db) as cursor:
            pkey = _pk_play(cursor, ids, tables=tables)
            q = q % (
                types._select_fields(types.Drive),
                _prefix_and(pkey),
                self._sorter.sql(tabtype=types.Drive),
            )
            cursor.execute(q)

            for row in cursor.fetchall():
                if (row['gsis_id'], row['drive_id']) in ids['drive']:
                    results.append(types.Drive.from_row(self._db, row))
        return results

    def _as_plays(self):
        """
        Executes the query and returns the results as a dictionary
        of `nlfdb.Play` objects that don't have the `play_player`
        attribute filled. The keys of the dictionary are play id
        tuples with the spec `(gsis_id, drive_id, play_id)`.

        The primary key membership SQL expression is also returned.
        """
        plays = OrderedDict()
        ids = self._ids('play', self._sorter)
        pset = _play_set(ids)
        pkey = None
        q = 'SELECT %s FROM play %s %s'

        tables = self._tables()
        tables.add('play')

        with Tx(self._db, factory=tuple_cursor) as cursor:
            pkey = _pk_play(cursor, ids, tables=tables)

            q = q % (
                types._select_fields(types.Play),
                _prefix_and(pkey),
                self._sorter.sql(tabtype=types.Play),
            )
            cursor.execute(q)
            init = types.Play._from_tuple
            for t in cursor.fetchall():
                pid = (t[0], t[1], t[2])
                if _in_play_set(pset, pid):
                    p = init(self._db, t)
                    plays[pid] = p
        return plays, pkey

    def as_plays(self, fill=True):
        """
        Executes the query and returns the results as a list of
        `nlfdb.Play` objects with the `nfldb.Play.play_players`
        attribute filled with player statistics.

        If `fill` is `False`, then player statistics will not be added
        to each `nfldb.Play` object returned. This can significantly
        speed things up if you don't need to access player statistics.

        Note that when `fill` is `False`, the `nfldb.Play.play_player`
        attribute is still available, but the data will be retrieved
        on-demand for each play. Also, if `fill` is `False`, then any
        sorting criteria specified to player statistics will be
        ignored.
        """
        self._sort_tables = [types.Play, types.PlayPlayer]
        plays, pkey = self._as_plays()
        if not fill:
            return plays.values()

        q = 'SELECT %s FROM play_player %s %s'
        with Tx(self._db, factory=tuple_cursor) as cursor:
            q = q % (
                types._select_fields(types.PlayPlayer),
                _prefix_and(pkey),
                self._sorter.sql(tabtype=types.PlayPlayer),
            )
            cursor.execute(q)
            init = types.PlayPlayer._from_tuple
            for t in cursor.fetchall():
                pid = (t[0], t[1], t[2])
                if pid in plays:
                    play = plays[pid]
                    if play._play_players is None:
                        play._play_players = []
                    play._play_players.append(init(self._db, t))
        return self._sorter.sorted(plays.values())

    def as_play_players(self):
        """
        Executes the query and returns the results as a list of
        `nlfdb.PlayPlayer` objects.

        This provides a way to access player statistics directly
        by bypassing play data. Usually the results of this method
        are passed to `nfldb.aggregate`.
        """
        self._sort_tables = [types.PlayPlayer]
        ids = self._ids('play_player', self._sorter)
        pset = _play_set(ids)
        player_pks = None
        tables = self._tables()
        tables.add('play_player')

        results = []
        q = 'SELECT %s FROM play_player %s %s'
        with Tx(self._db, factory=tuple_cursor) as cursor:
            pkey = _pk_play(cursor, ids, tables=tables)

            # Normally we wouldn't need to add this restriction on players,
            # but the identifiers in `ids` correspond to either plays or
            # players, and not their combination.
            if 'player' in tables or 'play_player':
                player_pks = _sql_pkey_in(cursor, ['player_id'], ids['player'])

            q = q % (
                types._select_fields(types.PlayPlayer),
                _prefix_and(player_pks, pkey),
                self._sorter.sql(tabtype=types.PlayPlayer),
            )
            cursor.execute(q)
            init = types.PlayPlayer._from_tuple
            for t in cursor.fetchall():
                pid = (t[0], t[1], t[2])
                if _in_play_set(pset, pid):
                    results.append(init(self._db, t))
        return results

    def as_players(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Player` objects.
        """
        self._sort_tables = [types.Player]
        ids = self._ids('player', self._sorter)
        results = []
        q = 'SELECT %s FROM player %s %s'
        with Tx(self._db) as cur:
            q = q % (
                types._select_fields(types.Player),
                _prefix_and(_sql_pkey_in(cur, ['player_id'], ids['player'])),
                self._sorter.sql(tabtype=types.Player),
            )
            cur.execute(q)

            for row in cur.fetchall():
                results.append(types.Player.from_row(self._db, row))
        return results

    def as_aggregate(self):
        """
        Executes the query and returns the results as aggregated
        `nfldb.PlayPlayer` objects. This method is meant to be a more
        restricted but much faster version of `nfldb.aggregate`.
        Namely, this method uses PostgreSQL to compute the aggregate
        statistics while `nfldb.aggregate` computes them in Python
        code.

        This method is slightly more restrictive in that you cannot
        specify criteria for searching by drive data. If any are
        specified when this method is called, an assertion error will
        be raised. This restriction may be relaxed in the future.

        If any sorting criteria is specified, it is applied to the
        aggregate *player* values only.
        """
        # The central approach here is to buck the trend of the other
        # `as_*` methods and do a JOIN to perform our search.
        # We do this because `IN` expressions are limited in the number
        # of sub-expressions they can contain, and since we can't do our
        # usual post-filtering with Python (since it's an aggregate),
        # we must resort to doing all the filtering in PostgreSQL.
        #
        # The only other option I can think of is to load the identifiers
        # into a temporary table and use a subquery with an `IN` expression,
        # which I'm told isn't subject to the normal limitations. However,
        # I'm not sure if it's economical to run a query against a big
        # table with so many `OR` expressions.
        #
        # Ideas and experiments are welcome. Using a join seems like the
        # most sensible approach at the moment, but I'd like to experiment
        # with other ideas in the future.
        #
        # N.B. I'm currently omitting the drive to avoid joining with
        # another table. Not a very good reason...
        tables, agg_tables = self._tables(), self._agg_tables()
        assert 'drive' not in tables, 'cannot use drive criteria in aggregate'

        gids, player_ids = None, None
        play_join = ''
        results = []

        # We can still use the `IN` expression with games and players.
        # Note that we don't sort identifiers at all, since sorting criteria
        # is only applied to aggregate values.
        ids = self._ids('', Sorter(None, None), ['game', 'player'])

        with Tx(self._db) as cur:
            if 'game' in tables:
                gids = _sql_pkey_in(cur, ['gsis_id'], ids['game'],
                                    prefix='play_player.')
            if 'player' in tables:
                player_ids = _sql_pkey_in(cur, ['player_id'], ids['player'],
                                          prefix='play_player.')
            if 'play' in tables or 'play' in agg_tables:
                play_join = '''
                    LEFT JOIN play
                    ON play_player.gsis_id = play.gsis_id
                        AND play_player.drive_id = play.drive_id
                        AND play_player.play_id = play.play_id
                '''

            where = self._sql_where(cur, ['play', 'play_player'])
            having = self._sql_where(cur, ['play', 'play_player'],
                                     prefix='', aggregate=True)
            q = '''
                SELECT play_player.player_id, {sum_fields}
                FROM play_player
                {play_join}
                {where}
                GROUP BY play_player.player_id
                {having}
                {order}
            '''.format(
                sum_fields=types._sum_fields(types.PlayPlayer),
                play_join=play_join,
                where=_prefix_and(gids, player_ids, where, prefix='WHERE '),
                having=_prefix_and(having, prefix='HAVING '),
                order=self._sorter.sql(tabtype=types.PlayPlayer, prefix=''),
            )
            cur.execute(q)

            fields = (types._player_categories.keys()
                      + types.PlayPlayer._sql_derived)
            for row in cur.fetchall():
                stats = {}
                for f in fields:
                    v = row[f]
                    if v != 0:
                        stats[f] = int(v)
                pp = types.PlayPlayer(self._db, None, None, None,
                                      row['player_id'], None, stats)
                results.append(pp)
        return results

    def _has_table(self, _):
        return True

    def _tables(self):
        tabs = set()
        for cond in self._andalso + self._orelse:
            tabs = tabs.union(cond._tables())
        return tabs

    def _agg_tables(self):
        tabs = set()
        for cond in self._agg_andalso + self._agg_orelse:
            tabs = tabs.union(cond._tables())
        return tabs

    def _sql_where(self, cur, tables, prefix=None, aggregate=False):
        if aggregate:
            return _sql_where(cur, tables, self._agg_andalso, self._agg_orelse,
                              prefix=prefix, aggregate=aggregate)
        else:
            return _sql_where(cur, tables, self._andalso, self._orelse,
                              prefix=prefix, aggregate=aggregate)

    def _ids(self, as_table, sorter, tables=None):
        """
        Returns a dictionary of primary keys matching the criteria
        specified in this query for the following tables: game, drive,
        play and player. The returned dictionary will have a key for
        each table with a corresponding `IdSet`, which may be empty
        or full.

        Each `IdSet` contains primary key values for that table. In the
        case of the `drive` and `play` table, those values are tuples.
        """
        # Initialize sets to "full". This distinguishes an empty result
        # set and a lack of search.
        ids = dict([(k, IdSet.full())
                    for k in ('game', 'drive', 'play', 'player')])

        # A list of fields for each table for easier access by table name.
        table_types = {
            'game': types.Game,
            'drive': types.Drive,
            'play': types.Play,
            'play_player': types.PlayPlayer,
            'player': types.Player,
        }

        def merge(add):
            for table, idents in ids.items():
                ids[table] = idents.intersection(add.get(table, IdSet.full()))

        def osql(table):
            if table != as_table:
                return ''
            return sorter.sql(tabtype=table_types[table], only_limit=True)

        def ids_game(cur):
            game = IdSet.empty()
            cur.execute('''
                SELECT gsis_id FROM game %s %s
            ''' % (_prefix_and(self._sql_where(cur, ['game'])), osql('game')))

            for row in cur.fetchall():
                game.add(row[0])
            return {'game': game}

        def ids_drive(cur):
            idexp = pkin(['gsis_id'], ids['game'])
            cur.execute('''
                SELECT gsis_id, drive_id FROM drive %s %s
            ''' % (_prefix_and(idexp, where('drive')), osql('drive')))

            game, drive = IdSet.empty(), IdSet.empty()
            for row in cur.fetchall():
                game.add(row[0])
                drive.add((row[0], row[1]))
            return {'game': game, 'drive': drive}

        def ids_play(cur):
            cur.execute('''
                SELECT gsis_id, drive_id, play_id FROM play %s %s
            ''' % (_prefix_and(_pk_play(cur, ids), where('play')),
                   osql('play')))
            pset = _play_set(ids)
            game, drive, play = IdSet.empty(), IdSet.empty(), IdSet.empty()
            for row in cur.fetchall():
                pid = (row[0], row[1], row[2])
                if not _in_play_set(pset, pid):
                    continue
                game.add(row[0])
                drive.add(pid[0:2])
                play.add(pid)
            return {'game': game, 'drive': drive, 'play': play}

        def ids_play_player(cur):
            cur.execute('''
                SELECT gsis_id, drive_id, play_id, player_id
                FROM play_player %s %s
            ''' % (_prefix_and(_pk_play(cur, ids), where('play_player')),
                   osql('play_player')))
            pset = _play_set(ids)
            game, drive, play = IdSet.empty(), IdSet.empty(), IdSet.empty()
            player = IdSet.empty()
            for row in cur.fetchall():
                pid = (row[0], row[1], row[2])
                if not _in_play_set(pset, pid):
                    continue
                game.add(row[0])
                drive.add(pid[0:2])
                play.add(pid)
                player.add(row[3])
            return {'game': game, 'drive': drive, 'play': play,
                    'player': player}

        def ids_player(cur):
            cur.execute('''
                SELECT player_id FROM player %s %s
            ''' % (_prefix_and(where('player')), osql('player')))
            player = IdSet.empty()
            for row in cur.fetchall():
                player.add(row[0])

            player_pks = pkin(['player_id'], player)
            cur.execute('''
                SELECT gsis_id, drive_id, play_id, player_id
                FROM play_player %s
            ''' % (_prefix_and(_pk_play(cur, ids), player_pks)))

            pset = _play_set(ids)
            game, drive, play = IdSet.empty(), IdSet.empty(), IdSet.empty()
            player = IdSet.empty()
            for row in cur.fetchall():
                pid = (row[0], row[1], row[2])
                if not _in_play_set(pset, pid):
                    continue
                game.add(row[0])
                drive.add(pid[0:2])
                play.add(pid)
                player.add(row[3])
            return {'game': game, 'drive': drive, 'play': play,
                    'player': player}

        with Tx(self._db, factory=tuple_cursor) as cur:
            def pkin(pkeys, ids, prefix=''):
                return _sql_pkey_in(cur, pkeys, ids, prefix=prefix)

            def where(table):
                return self._sql_where(cur, [table])

            def should_search(table):
                tabtype = table_types[table]
                return table in tables or sorter.is_restraining(tabtype)

            if tables is None:
                tables = self._tables()

            # Start with games since it has the smallest space.
            if should_search('game'):
                merge(ids_game(cur))
            if should_search('drive'):
                merge(ids_drive(cur))
            if should_search('play'):
                merge(ids_play(cur))
            if should_search('play_player'):
                merge(ids_play_player(cur))
            if should_search('player') or as_table == 'player':
                merge(ids_player(cur))
        return ids


class Sorter (object):
    """
    A representation of sort, order and limit criteria that can
    be applied in a SQL query or to a Python sequence.
    """
    @staticmethod
    def _normalize_order(order):
        order = order.upper()
        assert order in ('ASC', 'DESC'), 'order must be "asc" or "desc"'
        return order

    @staticmethod
    def cmp_to_key(mycmp):  # Taken from Python 2.7's functools
        """Convert a cmp= function into a key= function"""
        class K(object):
            __slots__ = ['obj']

            def __init__(self, obj, *args):
                self.obj = obj

            def __lt__(self, other):
                return mycmp(self.obj, other.obj) < 0

            def __gt__(self, other):
                return mycmp(self.obj, other.obj) > 0

            def __eq__(self, other):
                return mycmp(self.obj, other.obj) == 0

            def __le__(self, other):
                return mycmp(self.obj, other.obj) <= 0

            def __ge__(self, other):
                return mycmp(self.obj, other.obj) >= 0

            def __ne__(self, other):
                return mycmp(self.obj, other.obj) != 0

            def __hash__(self):
                raise TypeError('hash not implemented')
        return K

    def __init__(self, exprs=None, limit=None, restraining=[]):
        def normal_expr(e):
            if isinstance(e, strtype):
                return (e, 'DESC')
            elif isinstance(e, tuple):
                return (e[0], Sorter._normalize_order(e[1]))
            else:
                raise ValueError(
                    "Sortby expressions must be strings "
                    "or two-element tuples like (column, order). "
                    "Got value '%s' with type '%s'." % (e, type(e)))

        self.limit = int(limit or 0)
        self.exprs = []
        self.restraining = restraining
        if exprs is not None:
            if isinstance(exprs, strtype) or isinstance(exprs, tuple):
                self.exprs = [normal_expr(exprs)]
            else:
                for expr in exprs:
                    self.exprs.append(normal_expr(expr))

    def sorted(self, xs):
        """
        Sorts an iterable `xs` according to the criteria in `self`.

        If there are no sorting criteria specified, then this is
        equivalent to the identity function.
        """
        key = Sorter.cmp_to_key(self._cmp)
        if len(self.exprs) > 0:
            if self.limit > 0:
                xs = heapq.nsmallest(self.limit, xs, key=key)
            else:
                xs = sorted(xs, key=key)
        elif self.limit > 0:
            xs = xs[:self.limit]
        return xs

    def sql(self, tabtype, only_limit=False, prefix=None):
        """
        Return a SQL `ORDER BY ... LIMIT` expression corresponding to
        the criteria in `self`. If there are no ordering expressions
        in the sorting criteria, then an empty string is returned
        regardless of any limit criteria. (That is, specifying a limit
        requires at least one order expression.)

        If `fields` is specified, then only SQL columns in the sequence
        are used in the ORDER BY expression.

        If `only_limit` is `True`, then a SQL expression will only be
        returned if there is a limit of at least `1` specified in the
        sorting criteria. This is useful when an `ORDER BY` is only
        used to limit the results rather than influence an ordering
        returned to a client.

        The value of `prefix` is passed to the `tabtype._as_sql`
        function.
        """
        if only_limit and self.limit < 1:
            return ''

        exprs = self.exprs
        if tabtype is not None:
            exprs = [(f, o) for f, o in exprs if f in tabtype._sql_fields]
        if len(exprs) == 0:
            return ''

        as_sql = lambda f: tabtype._as_sql(f, prefix=prefix)
        s = ' ORDER BY '
        s += ', '.join('%s %s' % (as_sql(f), o) for f, o in exprs)
        if self.limit > 0:
            s += ' LIMIT %d' % self.limit
        return s

    def is_restraining(self, tabtype):
        """
        Returns `True` if and only if there exist sorting criteria
        *with* a limit that correspond to fields in the given table
        type.
        """
        if self.limit < 1:
            return False
        return tabtype in self.restraining

    def _cmp(self, a, b):
        compare, geta = cmp, getattr
        for field, order in self.exprs:
            x, y = geta(a, field, None), geta(b, field, None)
            if x is None or y is None:
                continue
            c = compare(x, y)
            if order == 'DESC':
                c *= -1
            if c != 0:
                return c
        return 0


class IdSet (object):
    """
    An incomplete wrapper for Python sets to represent collections
    of identifier sets. Namely, this allows for a set to be "full"
    so that every membership test returns `True` without actually
    storing every identifier.
    """
    @staticmethod
    def full():
        return IdSet(None)

    @staticmethod
    def empty():
        return IdSet([])

    def __init__(self, seq):
        if seq is None:
            self._set = None
        else:
            self._set = set(seq)

    @property
    def is_full(self):
        return self._set is None

    def add(self, x):
        if self._set is None:
            self._set = set()
        self._set.add(x)

    def intersection(self, s2):
        """
        Returns the intersection of two id sets, where either can be
        full.  Note that `s2` **must** be a `IdSet`, which differs from
        the standard library `set.intersection` function which can
        accept arbitrary sequences.
        """
        s1 = self
        if s1.is_full:
            return s2
        if s2.is_full:
            return s1
        return IdSet(s1._set.intersection(s2._set))

    def __contains__(self, x):
        if self.is_full:
            return True
        return x in self._set

    def __iter__(self):
        assert not self.is_full, 'cannot iterate on full set'
        return iter(self._set)

    def __len__(self):
        if self.is_full:
            return sys.maxint  # WTF? Maybe this should be an assert error?
        return len(self._set)
