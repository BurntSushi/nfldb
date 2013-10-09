from __future__ import absolute_import, division, print_function
from collections import defaultdict
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
    `PlayPlayer` objects with statistics aggregated (summed) over each
    player. (As a special case, if an element in `objs` is itself a
    `nfldb.PlayPlayer` object, then it is used and a `play_players`
    attribute is not rquired.)

    For example, `objs` could be a mixed list of `nfldb.Game` and
    `nfldb.Play` objects.

    The order of the list returned is stable with respect to the
    order of players obtained from each element in `objs`.

    It is recommended to use `nfldb.Query.aggregate` and
    `nfldb.Query.as_aggregate` instead of this function since summing
    statistics in the database is much faster. However, this function
    is provided for aggregation that cannot be expressed by the query
    interface.
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


def player_search(db, full_name, team=None, position=None, limit=1):
    """
    Given a database handle and a player's full name, this function
    searches the database for players with full names *similar* to the
    one given. Similarity is measured by the
    [Levenshtein distance](http://en.wikipedia.org/wiki/Levenshtein_distance).

    Results are returned as tuples. The first element is the is a
    `nfldb.Player` object and the second element is the Levenshtein
    distance. When `limit` is `1` (the default), then the return value
    is a tuple.  When `limit` is more than `1`, then the return value
    is a list of tuples.

    If no results are found, then `(None, None)` is returned when
    `limit == 1` or the empty list is returned when `limit > 1`.

    If `team` is not `None`, then only players **currently** on the
    team provided will be returned. Any players with an unknown team
    are therefore omitted.

    If `position` is not `None`, then only players **currently**
    at that position will be returned. Any players with an unknown
    position are therefore omitted.

    In order to use this function, the PostgreSQL `levenshtein`
    function must be available. If running this functions gives
    you an error about "No function matches the given name and
    argument types", then you can install the `levenshtein` function
    into your database by running the SQL query `CREATE EXTENSION
    fuzzystrmatch` as a superuser like `postgres`. For example:

        #!bash
        psql -U postgres -c 'CREATE EXTENSION fuzzystrmatch;' nfldb
    """
    assert isinstance(limit, int) and limit >= 1

    select_leven = 'levenshtein(full_name, %s) AS distance'
    q = '''
        SELECT %s, %s
        FROM player
        %s
        ORDER BY distance ASC LIMIT %d
    '''
    qteam, qposition = '', ''
    results = []
    with Tx(db) as cursor:
        if team is not None:
            qteam = cursor.mogrify('team = %s', (team,))
        if position is not None:
            qposition = cursor.mogrify('position = %s', (position,))

        select_leven = cursor.mogrify(select_leven, (full_name,))
        q = q % (
            types.select_columns(types.Player),
            select_leven,
            _prefix_and(qteam, qposition),
            limit
        )
        cursor.execute(q, (full_name,))

        for row in cursor.fetchall():
            results.append((types.Player.from_row(db, row), row['distance']))
    if limit == 1:
        if len(results) == 0:
            return (None, None)
        return results[0]
    return results


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
    """
    Returns a SQL IN expression of the form `(pkey1, pkey2, ..., pkeyN)
    IN ((val1, val2, ..., valN), ...)` where `pkeyi` is a member of
    the list `pkeys` and `(val1, val2, ..., valN)` is a member in the
    `nfldb.query.IdSet` `ids`.

    If `prefix` is set, then it is used as a prefix for each `pkeyi`.
    """
    pkeys = ['%s%s' % (prefix, pk) for pk in pkeys]
    if ids.is_full:
        return None
    elif len(ids) == 0:
        nulls = ', '.join(['NULL'] * len(pkeys))
        return '(%s) IN ((%s))' % (', '.join(pkeys), nulls)

    return '(%s) IN %s' % (', '.join(pkeys), cur.mogrify('%s', (tuple(ids),)))


def _pk_play(cur, ids, tables=['game', 'drive']):
    """
    A convenience function for calling `_sql_pkey_in` when selecting
    from the `play` or `play_player` tables. Namely, it only uses a
    SQL IN expression for the `nfldb.query.IdSet` `ids` when it has
    fewer than `nfldb.query._sql_max_in` values.

    `tables` should be a list of tables to specify which primary keys
    should be used. By default, only the `game` and `drive` tables
    are allowed, since they are usually within the limits of a SQL
    IN expression.
    """
    pk = None
    is_play = 'play' in tables or 'play_player' in tables
    if 'game' in tables and pk is None:
        pk = _sql_pkey_in(cur, ['gsis_id'], ids['game'])
    elif 'drive' in tables and len(ids['drive']) <= _sql_max_in:
        pk = _sql_pkey_in(cur, ['gsis_id', 'drive_id'], ids['drive'])
    elif is_play and len(ids['play']) <= _sql_max_in:
        pk = _sql_pkey_in(cur, ['gsis_id', 'drive_id', 'play_id'], ids['play'])
    return pk


def _play_set(ids):
    """
    Returns a value representing a set of plays in correspondence
    with the given `ids` dictionary mapping `play` or `drive` to
    `nfldb.query.IdSet`s. The value may be any combination of drive and
    play identifiers. Use `nfldb.query._in_play_set` for membership
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

    Valid values for `pset` can be constructed with
    `nfldb.query._play_set`.
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
    operators: `=`, `!=`, `<`, `<=`, `>` or `>=`. A value may be a list
    or a tuple, in which case PostgreSQL's `ANY` is used along with the
    given operator.
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
    """
    Creates a disjunctive `nfldb.Query` object, where every
    condition is combined disjunctively. Namely, it is an alias for
    `nfldb.Query(db, orelse=True)`.
    """
    return Query(db, orelse=True)


class Query (Condition):
    """
    A query represents a set of criteria to search nfldb's PostgreSQL
    database. Its primary feature is to provide a high-level API for
    searching NFL game, drive, play and player data very quickly.

    The basic workflow is to specify all of the search criteria that
    you want, and then use one of the `as_*` methods to actually
    perform the search and return results from the database.

    For example, to get all Patriots games as `nfldb.Game` objects from
    the 2012 regular season, we could do:

        #!python
        q = Query(db).game(season_year=2012, season_type='Regular', team='NE')
        for game in q.as_games():
            print game

    Other comparison operators like `<` or `>=` can also be used. To use
    them, append a suffix like `__lt` to the end of a field name. So to get
    all games with a home score greater than or equal to 50:

        #!python
        q = Query(db).game(home_score__ge=50)
        for game in q.as_games():
            print game

    Other suffixes are available: `__lt` for `<`, `__le` for `<=`,
    `__gt` for `>`, `__ge` for `>=`, `__ne` for `!=` and `__eq` for
    `==`. Although, the `__eq` suffix is used by default and is
    therefore never necessary to use.

    More criteria can be specified by chaining search criteria. For
    example, to get only plays as `nfldb.Play` objects where Tom Brady
    threw a touchdown pass:

        #!python
        q = Query(db).game(season_year=2012, season_type='Regular')
        q.player(full_name="Tom Brady").play(passing_tds=1)
        for play in q.as_plays():
            print play

    By default, all critera specified are combined conjunctively (i.e.,
    all criteria must be met for each result returned). However,
    sometimes you may want to specify disjunctive criteria (i.e., any
    of the criteria can be met for a result to be returned). To do this
    for a single field, simply use a list. For example, to get all
    Patriot games from the 2009 to 2013 seasons:

        #!python
        q = Query(db).game(season_type='Regular', team='NE')
        q.game(season_year=[2009, 2010, 2011, 2012, 2013])
        for game in q.as_games():
            print game

    Disjunctions can also be applied to multiple fields by creating a
    `nfldb.Query` object with `nfldb.QueryOR`. For example, to find
    all games where either team had more than 50 points:

        #!python
        q = QueryOR(db).game(home_score__ge=50, away_score__ge=50)
        for game in q.as_games():
            print game

    Finally, multiple queries can be combined with `nfldb.Query.andalso`.
    For example, to restrict the last search to games in the 2012 regular
    season:

        #!python
        big_score = QueryOR(db).game(home_score__ge=50, away_score__ge=50)

        q = Query(db).game(season_year=2012, season_type='Regular')
        q.andalso(big_score)
        for game in q.as_games():
            print game

    This is only the beginning of what can be done. More examples that run
    the gamut can be found on
    [nfldb's wiki](https://github.com/BurntSushi/nfldb/wiki).
    """

    def __init__(self, db, orelse=False):
        """
        Introduces a new `nfldb.Query` object. Criteria can be
        added with any combination of the `nfldb.Query.game`,
        `nfldb.Query.drive`, `nfldb.Query.play`, `nfldb.Query.player`
        and `nfldb.Query.aggregate` methods. Results can
        then be retrieved with any of the `as_*` methods:
        `nfldb.Query.as_games`, `nfldb.Query.as_drives`,
        `nfldb.Query.as_plays`, `nfldb.Query.as_play_players`,
        `nfldb.Query.as_players` and `nfldb.Query.as_aggregate`.

        Note that if aggregate criteria are specified with
        `nfldb.Query.aggregate`, then the **only** way to retrieve
        results is with the `nfldb.Query.as_aggregate` method. Invoking
        any of the other `as_*` methods will raise an assertion error.
        """

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
        """
        Specify sorting criteria for the result set returned by
        using sort expressions. A sort expression is a tuple with
        two elements: a field to sort by and the order to use. The
        field should correspond to an attribute of the objects you're
        returning and the order should be `asc` for ascending (smallest
        to biggest) or `desc` for descending (biggest to smallest).

        For example, `('passing_yds', 'desc')` would sort plays by the
        number of passing yards in the play, with the biggest coming
        first.

        Remember that a sort field must be an attribute of the
        results being returned. For example, you can't sort plays by
        `home_score`, which is an attribute of a `nfldb.Game` object.
        If you require this behavior, you will need to do it in Python
        with its `sorted` built in function. (Or alternatively, use
        two separate queries if the result set is large.)

        You may provide multiple sort expressions. For example,
        `[('gsis_id', 'asc'), ('time', 'asc'), ('play_id', 'asc')]`
        would sort plays in the order in which they occurred within
        each game.

        `exprs` may also just be a string specifying a single
        field which defaults to a descending order. For example,
        `sort('passing_yds')` sorts plays by passing yards in
        descending order.

        If `exprs` is set to the empty list, then sorting will be
        disabled for this query.

        Note that sorting criteria can be combined with
        `nfldb.Query.limit` to limit results which can dramatically
        speed up larger searches. For example, to fetch the top 10
        passing plays in the 2012 season:

            #!python
            q = Query(db).game(season_year=2012, season_type='Regular')
            q.sort('passing_yds').limit(10)
            for p in q.as_plays():
                print p

        A more naive approach might be to fetch all plays and sort them
        with Python:

            #!python
            q = Query(db).game(season_year=2012, season_type='Regular')
            plays = q.as_plays()

            plays = sorted(plays, key=lambda p: p.passing_yds, reverse=True)
            for p in plays[:10]:
                print p

        But this is over **43 times slower** on my machine than using
        `nfldb.Query.sort` and `nfldb.Query.limit`. (The performance
        difference is due to making PostgreSQL perform the search and
        restricting the number of results returned to process.)
        """
        self._sort_exprs = exprs
        return self

    def limit(self, count):
        """
        Limits the number of results to the integer `count`. If `count` is
        `0` (the default), then no limiting is done.

        See the documentation for `nfldb.Query.sort` for an example on how
        to combine it with `nfldb.Query.limit` to get results quickly.
        """
        self._limit = count
        return self

    @property
    def _sorter(self):
        return Sorter(self._sort_exprs, self._limit,
                      restraining=self._sort_tables)

    def _assert_no_aggregate(self):
        assert len(self._agg_andalso) == 0 and len(self._agg_orelse) == 0, \
            'aggregate criteria are only compatible with as_aggregate'

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
        query's list of disjunctive conditions.
        """
        self._orelse += conds
        return self

    def game(self, **kw):
        """
        Specify search criteria for an NFL game. The possible fields
        correspond to columns in the `game` table (or derived columns).
        They are documented as instance variables in the `nfldb.Game`
        class. Additionally, there are some special fields that provide
        convenient access to common conditions:

          * **team** - Find games that the team given played in, regardless
                       of whether it is the home or away team.

        Please see the documentation for `nfldb.Query` for examples on
        how to specify search criteria.

        Please
        [open an issue](https://github.com/BurntSushi/nfldb/issues/new)
        if you can think of other special fields to add.
        """
        _append_conds(self._default_cond, types.Game, kw)
        if 'team' in kw:
            ors = {'home_team': kw['team'], 'away_team': kw['team']}
            self.andalso(Query(self._db, orelse=True).game(**ors))
        return self

    def drive(self, **kw):
        """
        Specify search criteria for a drive. The possible fields
        correspond to columns in the `drive` table (or derived
        columns). They are documented as instance variables in the
        `nfldb.Drive` class.

        Please see the documentation for `nfldb.Query` for examples on
        how to specify search criteria.
        """
        _append_conds(self._default_cond, types.Drive, kw)
        return self

    def play(self, **kw):
        """
        Specify search criteria for a play. The possible fields
        correspond to columns in the `play` or `play_player` tables (or
        derived columns). They are documented as instance variables in
        the `nfldb.Play` and `nfldb.PlayPlayer` classes. Additionally,
        the fields listed on the
        [statistical categories](http://goo.gl/1qYG3C)
        wiki page may be used. That includes **both** `play` and
        `player` statistical categories.

        Please see the documentation for `nfldb.Query` for examples on
        how to specify search criteria.
        """
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
        """
        Specify search criteria for a player. The possible fields
        correspond to columns in the `player` table (or derived
        columns). They are documented as instance variables in the
        `nfldb.Player` class.

        Please see the documentation for `nfldb.Query` for examples on
        how to specify search criteria.
        """
        _append_conds(self._default_cond, types.Player, kw)
        return self

    def aggregate(self, **kw):
        """
        This is just like `nfldb.Query.play`, except the search
        parameters are applied to aggregate statistics.

        For example, to retrieve all quarterbacks who passed for at
        least 4000 yards in the 2012 season:

            #!python
            q = Query(db).game(season_year=2012, season_type='Regular')
            q.aggregate(passing_yds__ge=4000)
            for pp in q.as_aggregate():
                print pp.player, pp.passing_yds

        Aggregate results can also be sorted:

            #!python
            for pp in q.sort('passing_yds').as_aggregate():
                print pp.player, pp.passing_yds

        Note that this method can **only** be used with
        `nfldb.Query.as_aggregate`. Use with any of the other
        `as_*` methods will result in an assertion error. Note
        though that regular criteria can still be specified with
        `nfldb.Query.game`, `nfldb.Query.play`, etc. (Regular criteria
        restrict *what to aggregate* while aggregate criteria restrict
        *aggregated results*.)
        """
        _append_conds(self._agg_default_cond, types.Play, kw)
        _append_conds(self._agg_default_cond, types.PlayPlayer, kw)
        return self

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        self._assert_no_aggregate()

        self._sort_tables = [types.Game]
        ids = self._ids('game', self._sorter)
        results = []
        q = 'SELECT %s FROM game %s %s'
        with Tx(self._db) as cursor:
            q = q % (
                types.select_columns(types.Game),
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
        self._assert_no_aggregate()

        self._sort_tables = [types.Drive]
        ids = self._ids('drive', self._sorter)
        tables = self._tables()
        results = []
        q = 'SELECT %s FROM drive %s %s'
        with Tx(self._db) as cursor:
            pkey = _pk_play(cursor, ids, tables=tables)
            q = q % (
                types.select_columns(types.Drive),
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
        self._assert_no_aggregate()

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
                types.select_columns(types.Play),
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
        self._assert_no_aggregate()

        self._sort_tables = [types.Play, types.PlayPlayer]
        plays, pkey = self._as_plays()
        if not fill:
            return plays.values()

        q = 'SELECT %s FROM play_player %s %s'
        with Tx(self._db, factory=tuple_cursor) as cursor:
            q = q % (
                types.select_columns(types.PlayPlayer),
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
        are passed to `nfldb.aggregate`. It is recommended to use
        `nfldb.Query.aggregate` and `nfldb.Query.as_aggregate` when
        possible, since it is significantly faster to sum statistics in
        the database as opposed to Python.
        """
        self._assert_no_aggregate()

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
                types.select_columns(types.PlayPlayer),
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
        self._assert_no_aggregate()

        self._sort_tables = [types.Player]
        ids = self._ids('player', self._sorter)
        results = []
        q = 'SELECT %s FROM player %s %s'
        with Tx(self._db) as cur:
            q = q % (
                types.select_columns(types.Player),
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
        # table with so many `OR` expressions. More convincingly, the
        # approach I've used below seems to be *fast enough*.
        #
        # Ideas and experiments are welcome. Using a join seems like the
        # most sensible approach at the moment (and it's simple!), but I'd like
        # to experiment with other ideas in the future.
        tables, agg_tables = self._tables(), self._agg_tables()
        gids, player_ids = None, None
        joins = defaultdict(str)
        results = []

        with Tx(self._db) as cur:
            if 'game' in tables:
                joins['game'] = '''
                    LEFT JOIN game
                    ON play_player.gsis_id = game.gsis_id
                '''
            if 'drive' in tables:
                joins['drive'] = '''
                    LEFT JOIN drive
                    ON play_player.gsis_id = drive.gsis_id
                        AND play_player.drive_id = drive.drive_id
                '''
            if 'play' in tables or 'play' in agg_tables:
                joins['play'] = '''
                    LEFT JOIN play
                    ON play_player.gsis_id = play.gsis_id
                        AND play_player.drive_id = play.drive_id
                        AND play_player.play_id = play.play_id
                '''
            if 'player' in tables:
                joins['player'] = '''
                    LEFT JOIN player
                    ON play_player.player_id = player.player_id
                '''

            where = self._sql_where(cur, ['game', 'drive', 'play',
                                          'play_player', 'player'])
            having = self._sql_where(cur, ['play', 'play_player'],
                                     prefix='', aggregate=True)
            q = '''
                SELECT play_player.player_id, {sum_fields}
                FROM play_player
                {join_game}
                {join_drive}
                {join_play}
                {join_player}
                {where}
                GROUP BY play_player.player_id
                {having}
                {order}
            '''.format(
                sum_fields=types._sum_fields(types.PlayPlayer),
                join_game=joins['game'], join_drive=joins['drive'],
                join_play=joins['play'], join_player=joins['player'],
                where=_prefix_and(player_ids, where, prefix='WHERE '),
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
                        stats[f] = v
                pp = types.PlayPlayer(self._db, None, None, None,
                                      row['player_id'], None, stats)
                results.append(pp)
        return results

    def _tables(self):
        """Returns all the tables referenced in the search criteria."""
        tabs = set()
        for cond in self._andalso + self._orelse:
            tabs = tabs.union(cond._tables())
        return tabs

    def _agg_tables(self):
        """
        Returns all the tables referenced in the aggregate search criteria.
        """
        tabs = set()
        for cond in self._agg_andalso + self._agg_orelse:
            tabs = tabs.union(cond._tables())
        return tabs

    def show_where(self, aggregate=False):
        """
        Returns an approximate WHERE clause corresponding to the
        criteria specified in `self`. Note that the WHERE clause given
        is never explicitly used for performance reasons, but one hopes
        that it describes the criteria in `self`.

        If `aggregate` is `True`, then aggregate criteria for the
        `play` and `play_player` tables is shown with aggregate
        functions applied.
        """
        # Return criteria for all tables.
        tables = ['game', 'drive', 'play', 'play_player', 'player']
        with Tx(self._db) as cur:
            return self._sql_where(cur, tables, aggregate=aggregate)
        return ''

    def _sql_where(self, cur, tables, prefix=None, aggregate=False):
        """
        Returns a WHERE expression representing the search criteria
        in `self` and restricted to the tables in `tables`.

        If `aggregate` is `True`, then the appropriate aggregate
        functions are used.
        """
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
        # This method is where most of the complexity in this module lives,
        # since it is where most of the performance considerations are made.
        # Namely, the search criteria in `self` are spliced out by table
        # and used to find sets of primary keys for each table. The primary
        # keys are then used to filter subsequent searches on tables.
        #
        # The actual data returned is confined to the identifiers returned
        # from this method.

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
            if table == 'play_player' and as_table == 'play':
                # A special case to handle weird sorting issues since
                # some tables use the same column names.
                # When sorting plays, we only want to allow sorting on
                # player statistical fields and nothing else (like gsis_id,
                # play_id, etc.).
                player_stat = False
                for field, _ in sorter.exprs:
                    is_derived = field in types.PlayPlayer._sql_derived
                    if field in types._player_categories or is_derived:
                        player_stat = True
                        break
                if not player_stat:
                    return ''
            elif table != as_table:
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
            w = (_prefix_and(where('player')) + ' ' + osql('player')).strip()
            if not w:
                player = IdSet.full()
            else:
                cur.execute('SELECT player_id FROM player %s' % w)
                player = IdSet.empty()
                for row in cur.fetchall():
                    player.add(row[0])

            # Don't filter games/drives/plays/play_players if there is no
            # filter.
            if not _pk_play(cur, ids):
                return {'player': player}

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
                return where(table) or sorter.is_restraining(tabtype)

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
        if tabtype not in self.restraining:
            return False
        for field, _ in self.exprs:
            if field in tabtype._sql_fields:
                return True
        return False

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
