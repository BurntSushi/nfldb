from __future__ import absolute_import, division, print_function
from collections import defaultdict
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
import re

from psycopg2.extensions import cursor as tuple_cursor

from nfldb.db import Tx
import nfldb.sql as sql
import nfldb.types as types

try:
    strtype = basestring
except NameError:
    strtype = str


__pdoc__ = {}


_ENTITIES = {
    'game': types.Game,
    'drive': types.Drive,
    'play': types.Play,
    'play_player': types.PlayPlayer,
    'player': types.Player,
}


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


def _entities_by_ids(db, entity, *ids):
    """
    Given an `nfldb` `entity` like `nfldb.Play` and a list of tuples
    `ids` where each tuple is the primary key (or a subset of the
    primary key) for `entity`, return a list of instances of `entity`
    corresponding to the `ids` given.

    The order of the returned entities is undefined.
    """
    funs = {
        types.Game: {'query': Query.game, 'results': Query.as_games},
        types.Drive: {'query': Query.drive, 'results': Query.as_drives},
        types.Play: {'query': Query.play, 'results': Query.as_plays},
        types.PlayPlayer: {'query': Query.play_player,
                           'results': Query.as_play_players},
        types.Player: {'query': Query.player, 'results': Query.as_players},
    }[entity]
    q = Query(db)
    entq = funs['query']
    for pkey in ids:
        named = dict(zip(entity._sql_tables['primary'], pkey))
        q.orelse(entq(Query(db), **named))
    return funs['results'](q)


def player_search(db, full_name, team=None, position=None,
                  limit=1, soundex=False):
    """
    Given a database handle and a player's full name, this function
    searches the database for players with full names *similar* to the
    one given. Similarity is measured by the
    [Levenshtein distance](http://en.wikipedia.org/wiki/Levenshtein_distance),
    or by [Soundex similarity](http://en.wikipedia.org/wiki/Soundex).

    Results are returned as tuples. The first element is the is a
    `nfldb.Player` object and the second element is the Levenshtein
    (or Soundex) distance. When `limit` is `1` (the default), then the
    return value is a tuple.  When `limit` is more than `1`, then the
    return value is a list of tuples.

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

    Note that enabled the `fuzzystrmatch` extension also provides
    functions for comparing using Soundex.
    """
    assert isinstance(limit, int) and limit >= 1

    if soundex:
        # Careful, soundex distances are sorted in reverse of Levenshtein
        # distances.
        # Difference yields an integer in [0, 4].
        # A 4 is an exact match.
        fuzzy = 'difference(full_name, %s)'
        q = '''
            SELECT {columns}
            FROM player
            WHERE {where}
            ORDER BY distance DESC LIMIT {limit}
        '''
    else:
        fuzzy = 'levenshtein(full_name, %s)'
        q = '''
            SELECT {columns}
            FROM player
            WHERE {where}
            ORDER BY distance ASC LIMIT {limit}
        '''
    qteam, qposition = '', ''
    results = []
    with Tx(db) as cursor:
        if team is not None:
            qteam = cursor.mogrify('team = %s', (team,))
        if position is not None:
            qposition = cursor.mogrify('position = %s', (position,))

        fuzzy_filled = cursor.mogrify(fuzzy, (full_name,))
        columns = types.Player._sql_select_fields(types.Player.sql_fields())
        columns.append('%s AS distance' % fuzzy_filled)
        q = q.format(
            columns=', '.join(columns),
            where=sql.ands(fuzzy_filled + ' IS NOT NULL', qteam, qposition),
            limit=limit)
        cursor.execute(q, (full_name,))

        for row in cursor.fetchall():
            r = (types.Player.from_row_dict(db, row), row['distance'])
            results.append(r)
    if limit == 1:
        if len(results) == 0:
            return (None, None)
        return results[0]
    return results


def guess_position(pps):
    """
    Given a list of `nfldb.PlayPlayer` objects for the same player,
    guess the position of the player based on the statistics recorded.

    Note that this only distinguishes the offensive positions of QB,
    RB, WR, P and K. If defensive stats are detected, then the position
    returned defaults to LB.

    The algorithm used is simple majority vote. Whichever position is
    the most common is returned (and this may be `UNK`).
    """
    if len(pps) == 0:
        return types.Enums.player_pos.UNK

    counts = defaultdict(int)
    for pp in pps:
        counts[pp.guess_position] += 1
    return max(counts.items(), key=lambda (_, count): count)[0]


def _append_conds(conds, entity, kwargs):
    """
    Adds `nfldb.Condition` objects to the condition list `conds`
    for the `entity` type given. Only the values in `kwargs` that
    correspond to fields in `entity` are used.
    """
    allowed = set(entity.sql_fields())
    for k, v in kwargs.items():
        kbare = _no_comp_suffix(k)
        assert kbare in allowed, \
            "The key '%s' does not exist for entity '%s'." \
            % (kbare, entity.__name__)
        conds.append(Comparison(entity, k, v))


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


class Condition (object):
    """
    An abstract class that describes the interface of components
    in a SQL query.
    """
    def __init__(self):
        assert False, "Condition class cannot be instantiated."

    def _entities(self):
        """
        Returns a `set` of entity types, inheriting from
        `nfldb.Entity`, that are used in this condition.
        """
        assert False, "subclass responsibility"

    def _sql_where(self, cursor, aliases=None, aggregate=False):
        """
        Returns an escaped SQL string that can be safely substituted
        into the WHERE clause of a SELECT query for a particular.

        See the documentation for `nfldb.Entity` for information on
        the `aliases` parameter.

        If `aggregate` is `True`, then aggregate conditions should
        be used instead of regular conditions.
        """
        assert False, "subclass responsibility"

    @classmethod
    def _disjunctions(cls, cursor, disjuncts, aliases=None, aggregate=False):
        """
        Returns a valid SQL condition expression of the form:

            (d00 & d01 & ...) | (d10 & d11 & ...) | ...

        Where `d{N}` is a `nfldb.Condition` element in `disjuncts` and
        `d{Ni}` is an element in `d{N}`.
        """
        def sql(c):
            return c._sql_where(cursor, aliases=aliases, aggregate=aggregate)
        ds = []
        for conjuncts in disjuncts:
            ds.append(' AND '.join('(%s)' % sql(c) for c in conjuncts))
        return ' OR '.join('(%s)' % d for d in ds if d)


class Comparison (Condition):
    """
    A representation of a single comparison in a `nfldb.Query`.

    This corresponds to a field name, a value and one of the following
    operators: `=`, `!=`, `<`, `<=`, `>` or `>=`. A value may be a list
    or a tuple, in which case PostgreSQL's `ANY` is used along with the
    given operator.
    """

    def __init__(self, entity, kw, value):
        """
        Introduces a new condition given a user specified keyword `kw`
        with a `entity` (e.g., `nfldb.Play`) and a user provided
        value. The operator to be used is inferred from the suffix of
        `kw`. If `kw` has no suffix or a `__eq` suffix, then `=` is
        used. A suffix of `__ge` means `>=` is used, `__lt` means `<`,
        and so on.
        """
        self.operator = '='
        """The operator used in this condition."""

        self.entity = entity
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

    def _entities(self):
        return set([self.entity])

    def __str__(self):
        return '%s %s %s' \
               % (self.entity._sql_field(self.column),
                  self.operator, self.value)

    def _sql_where(self, cursor, aliases=None, aggregate=False):
        field = self.entity._sql_field(self.column, aliases=aliases)
        if aggregate:
            field = 'SUM(%s)' % field
        if isinstance(self.value, tuple) or isinstance(self.value, list):
            assert self.operator == '=', \
                'Disjunctions must use "=" for column "%s"' % field
            vals = [cursor.mogrify('%s', (v,)) for v in self.value]
            return '%s IN (%s)' % (field, ', '.join(vals))
        else:
            paramed = '%s %s %s' % (field, self.operator, '%s')
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

    def _sorter(self, default_entity):
        return Sorter(default_entity, self._sort_exprs, self._limit)

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
        if 'team' in kw:
            team = kw.pop('team')
            ors = {'home_team': team, 'away_team': team}
            self.andalso(Query(self._db, orelse=True).game(**ors))
        _append_conds(self._default_cond, types.Game, kw)
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
        return self

    def play_player(self, **kw):
        """
        Specify search criteria for individual play player statistics.
        The allowed fields are the columns in the `play_player`
        table.  They are documented as instance variables in the
        `nfldb.PlayPlayer` class. Additionally, the fields listed on
        the [statistical categories](http://goo.gl/1qYG3C) wiki page
        may be used. (Only the `player` statistical categories.)

        This method differs from `nfldb.Query.play` in that it can be
        used to select for individual player statistics in a play. In
        particular, there are *zero or more* player statistics for
        every play.
        """
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
                self.andalso(q.play_player(**ors))

            if nosuff in types.PlayPlayer._derived_combined:
                replace_or(*types.PlayPlayer._derived_combined[nosuff])
                kw.pop(field)

        # Now add the rest of the query.
        _append_conds(self._default_cond, types.PlayPlayer, kw)
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
        This is just like `nfldb.Query.play_player`, except the search
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
        _append_conds(self._agg_default_cond, types.PlayPlayer, kw)
        return self

    def _make_join_query(self, cursor, entity, only_prim=False, sorter=None,
                         ent_fillers=None):
        if sorter is None:
            sorter = self._sorter(entity)

        entities = self._entities()
        entities.update(sorter.entities)
        for ent in ent_fillers or []:
            entities.add(ent)
        entities.discard(entity)

        # If we're joining the `player` table with any other table except
        # `play_player`, then we MUST add `play_player` as a joining table.
        # It is the only way to bridge players and games/drives/plays.
        #
        # TODO: This could probably be automatically deduced in general case,
        # but we only have one case so just check for it manually.
        if (entity is not types.PlayPlayer and types.Player in entities) \
                or (entity is types.Player and len(entities) > 0):
            entities.add(types.PlayPlayer)

        if only_prim:
            columns = entity._sql_tables['primary']
            fields = entity._sql_select_fields(fields=columns)
        else:
            fields = []
            for ent in ent_fillers or []:
                fields += ent._sql_select_fields(fields=ent.sql_fields())
            fields += entity._sql_select_fields(fields=entity.sql_fields())
        args = {
            'columns': ', '.join(fields),
            'from': entity._sql_from(),
            'joins': entity._sql_join_all(entities),
            'where': sql.ands(self._sql_where(cursor)),
            'groupby': '',
            'sortby': sorter.sql(),
        }

        # We need a GROUP BY if we're joining with a table that has more
        # specific information. e.g., selecting from game with criteria
        # for plays.
        if any(entity._sql_relation_distance(to) > 0 for to in entities):
            fields = []
            for table, _ in entity._sql_tables['tables']:
                fields += entity._sql_primary_key(table)
            args['groupby'] = 'GROUP BY ' + ', '.join(fields)

        q = '''
            SELECT {columns} {from} {joins}
            WHERE {where}
            {groupby}
            {sortby}
        '''.format(**args)
        return q

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        self._assert_no_aggregate()

        results = []
        with Tx(self._db, factory=tuple_cursor) as cursor:
            q = self._make_join_query(cursor, types.Game)
            cursor.execute(q)
            for row in cursor.fetchall():
                results.append(types.Game.from_row_tuple(self._db, row))
        return results

    def as_drives(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Drive` objects.
        """
        self._assert_no_aggregate()

        results = []
        with Tx(self._db, factory=tuple_cursor) as cursor:
            q = self._make_join_query(cursor, types.Drive)
            cursor.execute(q)
            for row in cursor.fetchall():
                results.append(types.Drive.from_row_tuple(self._db, row))
        return results

    def as_plays(self, fill=True):
        """
        Executes the query and returns the results as a dictionary
        of `nlfdb.Play` objects that don't have the `play_player`
        attribute filled. The keys of the dictionary are play id
        tuples with the spec `(gsis_id, drive_id, play_id)`.

        The primary key membership SQL expression is also returned.
        """
        def make_pid(play):
            return (play.gsis_id, play.drive_id, play.play_id)

        self._assert_no_aggregate()

        # This is pretty terrifying.
        # Apparently PostgreSQL can change the order of rows returned
        # depending on the columns selected. So e.g., if you sort by `down`
        # and limit to 20 results, you might get a different 20 plays if
        # you change which columns you're selecting.
        # This is pertinent here because if we're filling plays with player
        # statistics, then we are assuming that this order never changes.
        # To make the ordering consistent, we add the play's primary key to
        # the existing sort criteria, which guarantees that the sort will
        # always be the same.
        # (We are careful not to override the user specified
        # `self._sort_exprs`.)
        #
        # That was a lie. We override the user settings if the user asks
        # to sort by `gsis_id`, `drive_id` or `play_id`.
        consistent = [(c, 'asc') for c in ['gsis_id', 'drive_id', 'play_id']]
        sorter = Sorter(types.Play, self._sort_exprs, self._limit)
        sorter.add_exprs(*consistent)

        if not fill:
            results = []
            with Tx(self._db, factory=tuple_cursor) as cursor:
                init = types.Play.from_row_tuple
                q = self._make_join_query(cursor, types.Play, sorter=sorter)
                cursor.execute(q)
                for row in cursor.fetchall():
                    results.append(init(self._db, row))
            return results
        else:
            plays = OrderedDict()
            with Tx(self._db, factory=tuple_cursor) as cursor:
                init_play = types.Play.from_row_tuple
                q = self._make_join_query(cursor, types.Play, sorter=sorter)
                cursor.execute(q)
                for row in cursor.fetchall():
                    play = init_play(self._db, row)
                    play._play_players = []
                    plays[make_pid(play)] = play

                # Run the above query *again* as a subquery.
                # This time, only fetch the primary key, and use that to
                # fetch all the `play_player` records in one swoop.
                aliases = {'play_player': 'pp'}
                ids = self._make_join_query(cursor, types.Play,
                                            only_prim=True, sorter=sorter)
                from_tables = types.PlayPlayer._sql_from(aliases=aliases)
                columns = types.PlayPlayer._sql_select_fields(
                    fields=types.PlayPlayer.sql_fields(), aliases=aliases)
                q = '''
                    SELECT {columns} {from_tables}
                    WHERE (pp.gsis_id, pp.drive_id, pp.play_id) IN ({ids})
                '''.format(columns=', '.join(columns),
                           from_tables=from_tables, ids=ids)

                init_pp = types.PlayPlayer.from_row_tuple
                cursor.execute(q)
                for row in cursor.fetchall():
                    pp = init_pp(self._db, row)
                    plays[make_pid(pp)]._play_players.append(pp)
            return plays.values()

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

        results = []
        with Tx(self._db, factory=tuple_cursor) as cursor:
            init = types.PlayPlayer.from_row_tuple
            q = self._make_join_query(cursor, types.PlayPlayer)
            cursor.execute(q)
            for row in cursor.fetchall():
                results.append(init(self._db, row))
        return results

    def as_players(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Player` objects.
        """
        self._assert_no_aggregate()

        results = []
        with Tx(self._db) as cursor:
            q = self._make_join_query(cursor, types.Player)
            cursor.execute(q)

            for row in cursor.fetchall():
                results.append(types.Player.from_row_dict(self._db, row))
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
        class AggPP (types.PlayPlayer):
            @classmethod
            def _sql_field(cls, name, aliases=None):

                if name in cls._derived_combined:
                    fields = cls._derived_combined[name]
                    fields = [cls._sql_field(f, aliases=aliases) for f in fields]
                    return ' + '.join(fields)
                elif name == 'points':
                    fields = ['(%s * %d)' % (cls._sql_field(f, aliases=aliases), pval)
                              for f, pval in cls._point_values]
                    return ' + '.join(fields)
                else:
                    sql = super(AggPP, cls)._sql_field(name, aliases=aliases)
                    return 'SUM(%s)' % sql

        joins = ''
        results = []
        with Tx(self._db) as cur:
            for ent in self._entities():
                if ent is types.PlayPlayer:
                    continue
                joins += types.PlayPlayer._sql_join_to_all(ent)

            sum_fields = types._player_categories.keys() \
                + AggPP._sql_tables['derived']
            select_sum_fields = AggPP._sql_select_fields(sum_fields)
            where = self._sql_where(cur)
            having = self._sql_where(cur, aggregate=True)
            q = '''
                SELECT
                    play_player.player_id AS play_player_player_id, {sum_fields}
                FROM play_player
                {joins}
                WHERE {where}
                GROUP BY play_player.player_id
                HAVING {having}
                {order}
            '''.format(
                sum_fields=', '.join(select_sum_fields),
                joins=joins,
                where=sql.ands(where),
                having=sql.ands(having),
                order=self._sorter(AggPP).sql(),
            )

            init = AggPP.from_row_dict
            cur.execute(q)
            for row in cur.fetchall():
                results.append(init(self._db, row))
        return results

    def _entities(self):
        """
        Returns all the entity types referenced in the search criteria.
        """
        tabs = set()
        for cond in self._andalso + self._orelse:
            tabs = tabs.union(cond._entities())
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
        with Tx(self._db) as cur:
            return self._sql_where(cur, aggregate=aggregate)
        return ''

    def _sql_where(self, cursor, aliases=None, aggregate=False):
        """
        Returns a WHERE expression representing the search criteria
        in `self` and restricted to the tables in `tables`.

        If `aggregate` is `True`, then the appropriate aggregate
        functions are used.
        """
        if aggregate:
            return Condition._disjunctions(
                cursor, [self._agg_andalso] + [[c] for c in self._agg_orelse],
                aliases=aliases, aggregate=aggregate)
        else:
            return Condition._disjunctions(
                cursor, [self._andalso] + [[c] for c in self._orelse],
                aliases=aliases, aggregate=aggregate)


class Sorter (object):
    """
    A representation of sort, order and limit criteria that can
    be applied in a SQL query.
    """
    @staticmethod
    def _normalize_order(order):
        order = order.upper()
        assert order in ('ASC', 'DESC'), 'order must be "asc" or "desc"'
        return order

    def __init__(self, default_entity, exprs=None, limit=None):
        self.default_entity = default_entity
        self.entities = set([default_entity])
        self.limit = int(limit or 0)
        self.exprs = []
        if isinstance(exprs, strtype) or isinstance(exprs, tuple):
            self.add_exprs(exprs)
        else:
            self.add_exprs(*(exprs or []))

    def add_exprs(self, *exprs):
        for e in exprs:
            e = self.normal_expr(e)
            self.entities.add(e[0])
            self.exprs.append(e)

    def normal_expr(self, e):
        if isinstance(e, strtype):
            return (self.default_entity, e, 'DESC')
        elif isinstance(e, tuple):
            assert len(e) == 2, 'invalid sort expression'
            return (self.default_entity, e[0], self._normalize_order(e[1]))
            # elif len(e) == 3:
                # assert e[0] in _ENTITIES, 'invalid entity: %s' % e[0]
                # self.entities.add(_ENTITIES[e[0]])
                # return (_ENTITIES[e[0]], e[1], self._normalize_order(e[2]))
        else:
            raise ValueError(
                "Sortby expressions must be strings "
                "or two-element tuples like (column, order). "
                "Got value '%s' with type '%s'." % (e, type(e)))

    def sql(self, aliases=None):
        """
        Return a SQL `ORDER BY ... LIMIT` expression corresponding to
        the criteria in `self`. If there are no ordering expressions
        in the sorting criteria, then an empty string is returned
        regardless of any limit criteria. (That is, specifying a limit
        requires at least one order expression.)

        The value of `prefix` is passed to the `tabtype._as_sql`
        function.
        """
        s = ''
        if len(self.exprs) > 0:
            sort_fields = []
            for ent, field, order in self.exprs:
                try:
                    field = ent._sql_field(field, aliases=aliases)
                except KeyError:
                    raise ValueError(
                        '%s is not a valid sort field for %s'
                        % (field, ent.__name__))
                sort_fields.append('%s %s' % (field, order))
            s += 'ORDER BY %s' % ', '.join(sort_fields)
        if self.limit > 0:
            s += ' LIMIT %d' % self.limit
        return ' ' + s + ' '
