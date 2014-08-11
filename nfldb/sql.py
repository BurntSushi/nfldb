from __future__ import absolute_import, division, print_function

from nfldb.db import _upsert


class Entity (object):
    """
    This is an abstract base class that handles most of the SQL
    plumbing for entities in `nfldb`. Its interface is meant to be
    declarative: specify the schema and let the methods defined here
    do the SQL generation work. However, it is possible to override
    methods (like `nfldb.Entity._sql_field`) when more customization
    is desired.

    Note that many of the methods defined here take an `aliases`
    argument. This should be a dictionary mapping table name (defined
    in `nfldb.Entity._sql_tables`) to some custom prefix. If it
    isn't provided, then the table name itself is used.
    """

    # This class doesn't introduce any instance variables, but we need
    # to declare as such, otherwise all subclasses will wind up with a
    # `__dict__`. (Thereby negating the benefit of using __slots__.)
    __slots__ = []

    _sql_tables = {}
    """
    A dictionary with four keys: `primary`, `tables`, `managed` and
    `derived`.

    The `primary` key should map to a list of primary key
    fields that correspond to a shared minimal subset of primary keys
    in all tables that represent this entity. (i.e., It should be the
    foreign key that joins all tables in the representation together.)

    The `tables` key should map to an association list of table names
    that map to lists of fields for that table. The lists of fields for
    every table should be *disjoint*: no two tables may share a field
    name in common (outside of the primary key).

    The `managed` key should be a list of tables that are managed
    directly by `nfldb`. `INSERT`, `UPDATE` and `DELETE` queries
    will be generated appropriately. (Tables not in this list are
    assumed to be maintained by the database itself, e.g., they are
    actually views or materialized views maintained by triggers.)

    The `derived` key should map to a list of *computed* fields. These
    are fields that aren't directly stored in the table, but can be
    computed from combining columns in the table (like `offense_tds` or
    `points`). This API will expose such fields as regular SQL columns
    in the API, and will handle writing them for you in `WHERE` and
    `ORDER BY` statements. The actual implementation of each computed
    field should be in an entity's `_sql_field` method (overriding the
    one defined on `nfldb.Entity`). The derived fields must be listed
    here so that the SQL generation code is aware of them.
    """

    @classmethod
    def _sql_columns(cls):
        """
        Returns all columns defined for this entity. Every field
        corresponds to a single column in a table.

        The first `N` columns returned correspond to this entity's
        primary key, where `N` is the number of columns in the
        primary key.
        """
        cols = cls._sql_tables['primary'][:]
        for table, table_cols in cls._sql_tables['tables']:
            cols += table_cols
        return cols

    @classmethod
    def sql_fields(cls):
        """
        Returns a list of all SQL fields across all tables for this
        entity, including derived fields. This method can be used
        in conjunction with `nfldb.Entity.from_row_tuple` to quickly
        create new `nfldb` objects without every constructing a dict.
        """
        if not hasattr(cls, '_cached_sql_fields'):
            cls._cached_sql_fields = cls._sql_columns()
            cls._cached_sql_fields += cls._sql_tables['derived']
        return cls._cached_sql_fields

    @classmethod
    def from_row_dict(cls, db, row):
        """
        Introduces a new entity object from a full SQL row result from
        the entity's tables. (i.e., `row` is a dictionary mapping
        column to value.) Note that the column names must be of the
        form '{entity_name}_{column_name}'. For example, in the `game`
        table, the `gsis_id` column must be named `game_gsis_id` in
        `row`.
        """
        obj = cls(db)
        seta = setattr
        prefix = cls._sql_primary_table() + '_'
        slice_from = len(prefix)
        for k in row:
            if k.startswith(prefix):
                seta(obj, k[slice_from:], row[k])
        return obj

    @classmethod
    def from_row_tuple(cls, db, t):
        """
        Given a tuple `t` corresponding to a result from a SELECT query,
        this will construct a new instance for this entity. Note that
        the tuple `t` must be in *exact* correspondence with the columns
        returned by `nfldb.Entity.sql_fields`.
        """
        cols = cls.sql_fields()
        seta = setattr
        obj = cls(db)
        for i, field in enumerate(cols):
            seta(obj, field, t[i])
        return obj

    @classmethod
    def _sql_from(cls, aliases=None):
        """
        Return a valid SQL `FROM table AS alias [LEFT JOIN extra_table
        ...]` string for this entity.
        """
        # This is a little hokey. Pick the first table as the 'FROM' table.
        # Subsequent tables are joined.
        from_table = cls._sql_primary_table()
        as_from_table = cls._sql_table_alias(from_table, aliases)

        extra_tables = ''
        for table, _ in cls._sql_tables['tables'][1:]:
            extra_tables += cls._sql_join_to(cls,
                                             from_table=from_table,
                                             to_table=table,
                                             from_aliases=aliases,
                                             to_aliases=aliases)
        return '''
            FROM {from_table} AS {as_from_table}
            {extra_tables}
        '''.format(from_table=from_table, as_from_table=as_from_table,
                   extra_tables=extra_tables)

    @classmethod
    def _sql_select_fields(cls, fields, wrap=None, aliases=None):
        """
        Returns correctly qualified SELECT expressions for each
        field in `fields` (namely, a field may be a derived field).

        If `wrap` is a not `None`, then it is applied to the result
        of calling `cls._sql_field` on each element in `fields`.

        All resulting fields are aliased with `AS` to correspond to
        the name given in `fields`. Namely, this makes table aliases
        opaque to the resulting query, but this also disallows
        selecting columns of the same name from multiple tables.
        """
        if wrap is None:
            wrap = lambda x: x
        sql = lambda f: wrap(cls._sql_field(f, aliases=aliases))
        entity_prefix = cls._sql_primary_table()
        return ['%s AS %s_%s' % (sql(f), entity_prefix, f) for f in fields]

    @classmethod
    def _sql_relation_distance(cls_from, cls_to):
        primf = set(cls_from._sql_tables['primary'])
        primt = set(cls_to._sql_tables['primary'])
        if len(primf.intersection(primt)) == 0:
            return None
        outsiders = primf.difference(primt).union(primt.difference(primf))
        if len(primf) > len(primt):
            return -len(outsiders)
        else:
            return len(outsiders)

    @classmethod
    def _sql_join_all(cls_from, cls_tos):
        """
        Given a list of sub classes `cls_tos` of `nfldb.Entity`,
        produce as many SQL `LEFT JOIN` clauses as is necessary so
        that all fields in all entity types given are available for
        filtering.

        Unlike the other join functions, this one has no alias support
        or support for controlling particular tables.

        The key contribution of this function is that it knows how to
        connect a group of tables correctly. e.g., If the group of
        tables is `game`, `play` and `play_player`, then `game` and
        `play` will be joined and `play` and `play_player` will be
        joined. (Instead of `game` and `play_player` or some other
        erronoeous combination.)

        In essence, each table is joined with the least general table
        in the group.
        """
        assert cls_from not in cls_tos, \
            'cannot join %s with itself with `sql_join_all`' % cls_from

        def dist(f, t):
            return f._sql_relation_distance(t)

        def relation_dists(froms, tos):
            return filter(lambda (f, t, d): d is not None,
                          ((f, t, dist(f, t)) for f in froms for t in tos))

        def more_general(froms, tos):
            return filter(lambda (f, t, d): d < 0, relation_dists(froms, tos))

        def more_specific(froms, tos):
            return filter(lambda (f, t, d): d > 0, relation_dists(froms, tos))

        joins = ''
        froms, tos = set([cls_from]), set(cls_tos)
        while len(tos) > 0:
            general = more_general(froms, tos)
            specific = more_specific(froms, tos)
            assert len(general) > 0 or len(specific) > 0, \
                'Cannot compute distances between sets. From: %s, To: %s' \
                % (froms, tos)

            def add_join(f, t):
                tos.discard(t)
                froms.add(t)
                return f._sql_join_to_all(t)
            if general:
                f, t, _ = max(general, key=lambda (f, t, d): d)
                joins += add_join(f, t)
            if specific:
                f, t, _ = min(specific, key=lambda (f, t, d): d)
                joins += add_join(f, t)
        return joins

    @classmethod
    def _sql_join_to_all(cls_from, cls_to, from_table=None,
                         from_aliases=None, to_aliases=None):
        """
        Given a **sub class** `cls_to` of `nfldb.Entity`, produce
        as many SQL `LEFT JOIN` clauses as is necessary so that all
        fields in `cls_to.sql_fields()` are available for filtering.

        See the documentation for `nfldb.Entity._sql_join_to` for
        information on the parameters.
        """
        to_primary = cls_to._sql_primary_table()
        joins = cls_from._sql_join_to(cls_to,
                                      from_table=from_table,
                                      to_table=to_primary,
                                      from_aliases=from_aliases,
                                      to_aliases=to_aliases)
        for table, _ in cls_to._sql_tables['tables'][1:]:
            joins += cls_to._sql_join_to(cls_to,
                                         from_table=to_primary,
                                         to_table=table,
                                         from_aliases=to_aliases,
                                         to_aliases=to_aliases)
        return joins

    @classmethod
    def _sql_join_to(cls_from, cls_to,
                     from_table=None, to_table=None,
                     from_aliases=None, to_aliases=None):
        """
        Given a **sub class** `cls_to` of `nfldb.Entity`, produce
        a SQL `LEFT JOIN` clause.

        If the primary keys in `cls_from` and `cls_to` have an empty
        intersection, then an assertion error is raised.

        Note that the first table defined for each of `cls_from` and
        `cls_to` is used to join them if `from_table` or `to_table`
        are `None`.

        `from_aliases` are only applied to the `from` tables and
        `to_aliases` are only applied to the `to` tables. This allows
        one to do self joins.
        """
        if from_table is None:
            from_table = cls_from._sql_primary_table()
        if to_table is None:
            to_table = cls_to._sql_primary_table()
        from_table = cls_from._sql_table_alias(from_table,
                                               aliases=from_aliases)
        as_to_table = cls_to._sql_table_alias(to_table, aliases=to_aliases)

        from_pkey = cls_from._sql_tables['primary']
        to_pkey = cls_to._sql_tables['primary']
        # Avoiding set.intersection so we can preserve order.
        common = [k for k in from_pkey if k in to_pkey]
        assert len(common) > 0, \
            "Cannot join %s to %s with non-overlapping primary keys." \
            % (cls_from.__name__, cls_to.__name__)
        fkey = [qualified_field(from_table, f) for f in common]
        tkey = [qualified_field(as_to_table, f) for f in common]
        return '''
            LEFT JOIN {to_table} AS {as_to_table}
            ON ({fkey}) = ({tkey})
        '''.format(to_table=to_table, as_to_table=as_to_table,
                   fkey=', '.join(fkey), tkey=', '.join(tkey))

    @classmethod
    def _sql_primary_key(cls, table, aliases=None):
        t = cls._sql_table_alias(table, aliases)
        return [qualified_field(t, f)
                for f in cls._sql_tables['primary']]

    @classmethod
    def _sql_primary_table(cls):
        return cls._sql_tables['tables'][0][0]

    @classmethod
    def _sql_column_to_table(cls, name):
        """
        Returns the table in `cls._sql_tables` containing the
        field `name`.

        If `name` corresponds to a primary key column, then
        the primary table (first table) is returned.

        If a table could not be found, a `exceptions.KeyError` is
        raised.
        """
        if name in cls._sql_tables['primary']:
            return cls._sql_primary_table()
        for table_name, fields in cls._sql_tables['tables']:
            if name in fields:
                return table_name
        raise KeyError("Could not find table for %s" % name)

    @classmethod
    def _sql_table_alias(cls, table_name, aliases):
        if aliases is None or table_name not in aliases:
            return table_name
        else:
            return aliases[table_name]

    @classmethod
    def _sql_field(cls, name, aliases=None):
        """
        Returns a SQL expression corresponding to the field `name`.

        The default implementation returns `table_for_name`.`name`.

        Entities can override this for special computed fields.
        """
        prefix = cls._sql_table_alias(cls._sql_column_to_table(name), aliases)
        return qualified_field(prefix, name)

    def _save(self, cursor):
        """
        Does an upsert for each managed table specified in
        `nfldb.Entity._sql_tables`. The data is drawn from
        `self`.
        """
        for table, prim, vals in self._rows:
            _upsert(cursor, table, vals, prim)

    @property
    def _rows(self):
        prim = self._sql_tables['primary'][:]
        for table, table_fields in self._sql_tables['tables']:
            if table in self._sql_tables['managed']:
                r = _as_row(prim + table_fields, self)
                yield table, r[0:len(prim)], r


def _as_row(fields, obj):
    """
    Given a list of fields in a SQL table and a Python object, return
    an association list where the keys are from `fields` and the values
    are the result of `getattr(obj, fields[i], None)` for some `i`.

    Note that the `time_inserted` and `time_updated` fields are always
    omitted.
    """
    exclude = ('time_inserted', 'time_updated')
    return [(f, getattr(obj, f, None)) for f in fields if f not in exclude]


def ands(*exprs):
    anded = ' AND '.join('(%s)' % e for e in exprs if e)
    return 'true' if len(anded) == 0 else anded


def qualified_field(alias, field):
    """
    Qualifies the SQL `field` with `alias`. If `alias` is empty,
    then no qualification is used. (Just `field` is returned.)
    """
    if not alias:
        return field
    else:
        return '%s.%s' % (alias, field)
