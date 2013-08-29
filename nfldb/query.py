from __future__ import absolute_import, division, print_function
import re

from nfldb.db import Tx
import nfldb.types as types

try:
    strtype = basestring
except NameError:  # I have lofty hopes for Python 3.
    strtype = str


__pdoc__ = {}


def _select_fields(prefix, fields):
    return ', '.join('%s.%s' % (prefix, field) for field in fields)


def _append_conds(conds, table, keys, kwargs):
    """
    Adds `nfldb.Condition` objects to the condition list `conds` for
    the `table`. Only the values in `kwargs` that correspond to keys in
    `keys` are used.
    """
    def sanitize(s):
        return re.sub('__(eq|ne|gt|lt|ge|le)$', '', s)
    for k, v in ((k, v) for k, v in kwargs.items() if sanitize(k) in keys):
        conds.append(Comparison(table, k, v))


class Condition (object):
    """
    An abstract class that describes the interface of components
    in a SQL query.
    """
    def __init__(self):
        assert False, "Condition class cannot be instantiated."

    def _has_table(self, table):
        """
        Returns `True` if and only if the given condition belongs to
        a single `table`. For `nfldb.Query` objects, this is always
        `True`, since they can encompass multiple tables.
        """
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

    def _has_table(self, table):
        return self.table == table

    def _sql_where(self, cursor, table):
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

    def players(self, **kw):
        _append_conds(self._default_cond, 'player',
                      types.Player._sql_fields, kw)
        return self

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        results = []
        with Tx(self._db) as cursor:
            ids = self._ids(cursor)
            q = 'SELECT %s FROM game %s' \
                % (_select_fields('game', types.Game._sql_fields),
                   self._sql_pkey_where(cursor, 'gsis_id', ids['game']))
            cursor.execute(q)

            for row in cursor.fetchall():
                results.append(types.Game.from_row(self._db, row))
        return results

    def _has_table(self, _):
        return True

    def _sql_where(self, cur, table):
        disjunctions = []
        andsql = ' AND '.join(c._sql_where(cur, table)
                              for c in self._andalso if c._has_table(table))
        if len(andsql) > 0:
            andsql = '(%s)' % andsql
            disjunctions.append(andsql)
        disjunctions += [c._sql_where(cur, table)
                         for c in self._orelse if c._has_table(table)]
        
        if len(disjunctions) == 0:
            return ''
        return '(%s)' % (' OR '.join(disjunctions))

    def _sql_pkey_where(self, cur, pkey, ids):
        if len(ids) == 0:
            return ''
        r = 'WHERE %s IN %s' % (pkey, cur.mogrify('%s', (ids,)))
        return r

    def _ids(self, cur):
        """
        Returns a dictionary of primary keys matching the criteria
        specified in this query for the following tables: game, drive,
        play and player. The returned dictionary will have a key for
        each table with a corresponding tuple, which may be empty.

        Each tuple contains primary key values for that table. In the
        case of the `drive` and `play` table, those values are tuples.
        """
        game, drive, play, player = set(), set(), set(), set()

        # Start with games since it has the smallest space.
        cur.execute('SELECT gsis_id FROM game WHERE %s'
                    % self._sql_where(cur, 'game'))
        for row in cur.fetchall():
            game.add(row['gsis_id'])

        # Now move ont
        return {
            'game': tuple(game), 'drive': tuple(drive),
            'play': tuple(play), 'player': tuple(player),
        }
