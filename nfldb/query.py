from collections import defaultdict

from nfldb.db import Tx
import nfldb.types as types


__pdoc__ = {}


def _prefix_join_fields(prefix, fields):
    return ', '.join('%s.%s' % (prefix, field) for field in fields)


class Condition (object):
    """
    A representation of a single condition in a `nfldb.Query`.

    This corresponds to a field name, a value and one of the following
    operators: `=`, `!=`, `<`, `<=`, `>` or `>=`.
    """
    def __init__(self, kw, value, prefix=None):
        """
        Introduces a new condition given a user specified keyword `kw`
        with a table prefix `prefix` and a user provided value. The
        operator to be used is inferred from the suffix of `kw`. If
        `kw` has no suffix or a `__eq` suffix, then `=` is used. A
        suffix of `__ge` means `>=` is used. And so on.
        """
        self.operator = '='
        """The operator used in this condition."""

        self.prefix = prefix
        """The table prefix for this column. It may be empty."""

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

    def __str__(self):
        """
        Returns a parameterized SQL string for this condition.
        """
        prefix = ('%s.' % self.prefix) if self.prefix else ''
        return '%s%s %s %s' % (prefix, self.column, self.operator, '%s')


class Query (object):
    """
    A query represents a set of criteria to search a database.
    """

    def __init__(self, db):
        self._db = db
        """A psycopg2 database connection object."""

        self._conds = defaultdict(list)
        """
        A mapping from table name to list of conjunctive conditions.
        """

    def _mogrify_conds(self, cursor, conds):
        """
        Returns valid and escaped SQL for use in a WHERE clause
        of a list of conditions.

        The cursor is necessary for mogrification.
        """
        if len(conds) == 0:
            return 'True'  # Sneaky. Don't fiddle with "WHERE".
        mog = lambda cond: cursor.mogrify(str(cond), (cond.value,))
        return ' AND '.join(mog(cond) for cond in conds)

    def games(self, **kw):
        """
        Specify search criteria for an NFL game. The possible fields
        correspond to columns in the SQL table. They are documented
        as instance variables in the `nfldb.Game` class.
        """
        for k, v in kw.items():
            self._conds['game'].append(Condition(k, v, prefix='game'))
        return self

    def as_games(self):
        """
        Executes the query and returns the results as a list of
        `nfldb.Game` objects.
        """
        results = []
        with Tx(self._db) as cursor:
            q = 'SELECT %s FROM game WHERE %s' \
                % (_prefix_join_fields('game', types.Game._sql_fields),
                   self._mogrify_conds(cursor, self._conds['game']))
            cursor.execute(q)
            for row in cursor.fetchall():
                results.append(types.Game.from_row(self._db, row))
        return results
