from __future__ import absolute_import, division, print_function
import ConfigParser
import datetime
import os
import os.path as path
import re
import sys

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
from psycopg2.extensions import new_type, register_type

import pytz

import nfldb.team

__pdoc__ = {}

api_version = 7
__pdoc__['api_version'] = \
    """
    The schema version that this library corresponds to. When the schema
    version of the database is less than this value, `nfldb.connect` will
    automatically update the schema to the latest version before doing
    anything else.
    """

_SHOW_QUERIES = False
"""When set, all queries will be printed to stderr."""

_NUM_QUERIES = 0
"""
The total number of queries executed. Only updated when _SHOW_QUERIES
is true.
"""

_config_home = os.getenv('XDG_CONFIG_HOME')
if not _config_home:
    home = os.getenv('HOME')
    if not home:
        _config_home = ''
    else:
        _config_home = path.join(home, '.config')


def config(config_path=''):
    """
    Reads and loads the configuration file containing PostgreSQL
    connection information. This function is used automatically
    by `nfldb.connect`.

    The return value is a tuple. The first value is a dictionary
    mapping a key in the configuration file to its corresponding
    value. All values are strings, except for `port`, which is always
    an integer. The second value is a list of paths that were searched
    to find a config file. (When one is found, the last element in
    this list corresponds to the actual location of the config file.)

    A total of three possible file paths are tried before giving
    up and returning `None`. The file paths, in order, are:
    `config_path`, `sys.prefix/share/nfldb/config.ini` and
    `$XDG_CONFIG_HOME/nfldb/config.ini`.
    """
    paths = [
        config_path,
        path.join(sys.prefix, 'share', 'nfldb', 'config.ini'),
        path.join(_config_home, 'nfldb', 'config.ini'),
    ]
    tried = []
    cp = ConfigParser.RawConfigParser()
    for p in paths:
        tried.append(p)
        try:
            with open(p) as fp:
                cp.readfp(fp)
                return {
                    'timezone': cp.get('pgsql', 'timezone'),
                    'database': cp.get('pgsql', 'database'),
                    'user': cp.get('pgsql', 'user'),
                    'password': cp.get('pgsql', 'password'),
                    'host': cp.get('pgsql', 'host'),
                    'port': cp.getint('pgsql', 'port'),
                }, tried
        except IOError:
            pass
    return None, tried


def connect(database=None, user=None, password=None, host=None, port=None,
            timezone=None, config_path=''):
    """
    Returns a `psycopg2._psycopg.connection` object from the
    `psycopg2.connect` function. If database is `None`, then `connect`
    will look for a configuration file using `nfldb.config` with
    `config_path`. Otherwise, the connection will use the parameters
    given.

    If `database` is `None` and no config file can be found, then an
    `IOError` exception is raised.

    This function will also compare the current schema version of the
    database against the API version `nfldb.api_version` and assert
    that they are equivalent. If the schema library version is less
    than the the API version, then the schema will be automatically
    upgraded. If the schema version is newer than the library version,
    then this function will raise an assertion error. An assertion
    error will also be raised if the schema version is 0 and the
    database is not empty.

    N.B. The `timezone` parameter should be set to a value that
    PostgreSQL will accept. Select from the `pg_timezone_names` view
    to get a list of valid time zones.
    """
    if database is None:
        conf, tried = config(config_path=config_path)
        if conf is None:
            raise IOError("Could not find valid configuration file. "
                          "Tried the following paths: %s" % tried)

        timezone, database = conf['timezone'], conf['database']
        user, password = conf['user'], conf['password']
        host, port = conf['host'], conf['port']

    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port)

    # Start the migration. Make sure if this is the initial setup that
    # the DB is empty.
    sversion = schema_version(conn)
    assert sversion <= api_version, \
        'Library with version %d is older than the schema with version %d' \
        % (api_version, sversion)
    assert sversion > 0 or (sversion == 0 and _is_empty(conn)), \
        'Schema has version 0 but is not empty.'
    set_timezone(conn, 'UTC')
    _migrate(conn, api_version)

    if timezone is not None:
        set_timezone(conn, timezone)

    # Bind SQL -> Python casting functions.
    from nfldb.types import Clock, _Enum, Enums, FieldPosition, PossessionTime
    _bind_type(conn, 'game_phase', _Enum._pg_cast(Enums.game_phase))
    _bind_type(conn, 'season_phase', _Enum._pg_cast(Enums.season_phase))
    _bind_type(conn, 'game_day', _Enum._pg_cast(Enums.game_day))
    _bind_type(conn, 'player_pos', _Enum._pg_cast(Enums.player_pos))
    _bind_type(conn, 'player_status', _Enum._pg_cast(Enums.player_status))
    _bind_type(conn, 'game_time', Clock._pg_cast)
    _bind_type(conn, 'pos_period', PossessionTime._pg_cast)
    _bind_type(conn, 'field_pos', FieldPosition._pg_cast)

    return conn


def schema_version(conn):
    """
    Returns the schema version of the given database. If the version
    is not stored in the database, then `0` is returned.
    """
    with Tx(conn) as c:
        try:
            c.execute('SELECT version FROM meta LIMIT 1', ['version'])
        except psycopg2.ProgrammingError:
            return 0
        if c.rowcount == 0:
            return 0
        return c.fetchone()['version']


def set_timezone(conn, timezone):
    """
    Sets the timezone for which all datetimes will be displayed
    as. Valid values are exactly the same set of values accepted
    by PostgreSQL. (Select from the `pg_timezone_names` view to
    get a list of valid time zones.)

    Note that all datetimes are stored in UTC. This setting only
    affects how datetimes are viewed from select queries.
    """
    with Tx(conn) as c:
        c.execute('SET timezone = %s', (timezone,))


def now():
    """
    Returns the current date/time in UTC as a `datetime.datetime`
    object. It can be used to compare against date/times in any of the
    `nfldb` objects without worrying about timezones.
    """
    return datetime.datetime.now(pytz.utc)


def _bind_type(conn, sql_type_name, cast):
    """
    Binds a `cast` function to the SQL type in the connection `conn`
    given by `sql_type_name`. `cast` must be a function with two
    parameters: the SQL value and a cursor object. It should return the
    appropriate Python object.

    Note that `sql_type_name` is not escaped.
    """
    with Tx(conn) as c:
        c.execute('SELECT NULL::%s' % sql_type_name)
        typ = new_type((c.description[0].type_code,), sql_type_name, cast)
        register_type(typ)


def _db_name(conn):
    m = re.search('dbname=(\S+)', conn.dsn)
    return m.group(1)


def _is_empty(conn):
    """
    Returns `True` if and only if there are no tables in the given
    database.
    """
    with Tx(conn) as c:
        c.execute('''
            SELECT COUNT(*) AS count FROM information_schema.tables
            WHERE table_catalog = %s AND table_schema = 'public'
        ''', [_db_name(conn)])
        if c.fetchone()['count'] == 0:
            return True
    return False


def _mogrify(cursor, xs):
    """Shortcut for mogrifying a list as if it were a tuple."""
    return cursor.mogrify('%s', (tuple(xs),))


def _num_rows(cursor, table):
    """Returns the number of rows in table."""
    cursor.execute('SELECT COUNT(*) AS rowcount FROM %s' % table)
    return cursor.fetchone()['rowcount']


class Tx (object):
    """
    Tx is a `with` compatible class that abstracts a transaction given
    a connection. If an exception occurs inside the `with` block, then
    rollback is automatically called. Otherwise, upon exit of the with
    block, commit is called.

    Tx blocks can be nested inside other Tx blocks. Nested Tx blocks
    never commit or rollback a transaction. Instead, the exception is
    passed along to the caller. Only the outermost transaction will
    commit or rollback the entire transaction.

    Use it like so:

        #!python
        with Tx(conn) as cursor:
            ...

    Which is meant to be roughly equivalent to the following:

        #!python
        with conn:
            with conn.cursor() as curs:
                ...

    This should only be used when you're running SQL queries directly.
    (Or when interfacing with another part of the API that requires
    a database cursor.)
    """
    def __init__(self, psycho_conn, name=None, factory=None):
        """
        `psycho_conn` is a DB connection returned from `nfldb.connect`,
        `name` is passed as the `name` argument to the cursor
        constructor (for server-side cursors), and `factory` is passed
        as the `cursor_factory` parameter to the cursor constructor.

        Note that the default cursor factory is
        `psycopg2.extras.RealDictCursor`. However, using
        `psycopg2.extensions.cursor` (the default tuple cursor) can be
        much more efficient when fetching large result sets.
        """
        tstatus = psycho_conn.get_transaction_status()
        self.__name = name
        self.__nested = tstatus == TRANSACTION_STATUS_INTRANS
        self.__conn = psycho_conn
        self.__cursor = None
        self.__factory = factory
        if self.__factory is None:
            self.__factory = RealDictCursor

    def __enter__(self):
        # No biscuits for the psycopg2 author. Changed the public API in
        # 2.5 in a very very subtle way.
        # In 2.4, apparently `name` cannot be `None`. Why? I don't know.
        if self.__name is None:
            self.__cursor = self.__conn.cursor(cursor_factory=self.__factory)
        else:
            self.__cursor = self.__conn.cursor(self.__name, self.__factory)
        c = self.__cursor

        if _SHOW_QUERIES:
            class _ (object):
                def execute(self, *args, **kwargs):
                    global _NUM_QUERIES

                    _NUM_QUERIES += 1
                    c.execute(*args, **kwargs)
                    print(c.query, file=sys.stderr, end='\n\n')

                def __getattr__(self, k):
                    return getattr(c, k)
            return _()
        else:
            return c

    def __exit__(self, typ, value, traceback):
        if not self.__cursor.closed:
            self.__cursor.close()
        if typ is not None:
            if not self.__nested:
                self.__conn.rollback()
            return False
        else:
            if not self.__nested:
                self.__conn.commit()
            return True


def _big_insert(cursor, table, datas):
    """
    Given a database cursor, table name and a list of asssociation
    lists of data (column name and value), perform a single large
    insert. Namely, each association list should correspond to a single
    row in `table`.

    Each association list must have exactly the same number of columns
    in exactly the same order.
    """
    stamped = table in ('game', 'drive', 'play')
    insert_fields = [k for k, _ in datas[0]]
    if stamped:
        insert_fields.append('time_inserted')
        insert_fields.append('time_updated')
    insert_fields = ', '.join(insert_fields)

    def times(xs):
        if stamped:
            xs.append('NOW()')
            xs.append('NOW()')
        return xs

    def vals(xs):
        return [v for _, v in xs]
    values = ', '.join(_mogrify(cursor, times(vals(data))) for data in datas)

    cursor.execute('INSERT INTO %s (%s) VALUES %s'
                   % (table, insert_fields, values))


def _upsert(cursor, table, data, pk):
    """
    Performs an arbitrary "upsert" given a table, an association list
    mapping key to value, and an association list representing the
    primary key.

    Note that this is **not** free of race conditions. It is the
    caller's responsibility to avoid race conditions. (e.g., By using a
    table or row lock.)

    If the table is `game`, `drive` or `play`, then the `time_insert`
    and `time_updated` fields are automatically populated.
    """
    stamped = table in ('game', 'drive', 'play')
    update_set = ['%s = %s' % (k, '%s') for k, _ in data]
    if stamped:
        update_set.append('time_updated = NOW()')
    update_set = ', '.join(update_set)

    insert_fields = [k for k, _ in data]
    insert_places = ['%s' for _ in data]
    if stamped:
        insert_fields.append('time_inserted')
        insert_fields.append('time_updated')
        insert_places.append('NOW()')
        insert_places.append('NOW()')
    insert_fields = ', '.join(insert_fields)
    insert_places = ', '.join(insert_places)

    pk_cond = ' AND '.join(['%s = %s' % (k, '%s') for k, _ in pk])
    q = '''
        UPDATE %s SET %s WHERE %s;
    ''' % (table, update_set, pk_cond)
    q += '''
        INSERT INTO %s (%s)
        SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s)
    ''' % (table, insert_fields, insert_places, table, pk_cond)

    values = [v for _, v in data]
    pk_values = [v for _, v in pk]
    try:
        cursor.execute(q, values + pk_values + values + pk_values)
    except psycopg2.ProgrammingError as e:
        print(cursor.query)
        raise e


def _drop_stat_indexes(c):
    from nfldb.types import _play_categories, _player_categories

    for cat in _player_categories.values():
        c.execute('DROP INDEX play_player_in_%s' % cat)
    for cat in _play_categories.values():
        c.execute('DROP INDEX play_in_%s' % cat)


def _create_stat_indexes(c):
    from nfldb.types import _play_categories, _player_categories

    for cat in _player_categories.values():
        c.execute('CREATE INDEX play_player_in_%s ON play_player (%s ASC)'
                  % (cat, cat))
    for cat in _play_categories.values():
        c.execute('CREATE INDEX play_in_%s ON play (%s ASC)' % (cat, cat))


# What follows are the migration functions. They follow the naming
# convention "_migrate_{VERSION}" where VERSION is an integer that
# corresponds to the version that the schema will be after the
# migration function runs. Each migration function is only responsible
# for running the queries required to update schema. It does not
# need to update the schema version.
#
# The migration functions should accept a cursor as a parameter,
# which is created in the _migrate function. In particular,
# each migration function is run in its own transaction. Commits
# and rollbacks are handled automatically.


def _migrate(conn, to):
    current = schema_version(conn)
    assert current <= to

    globs = globals()
    for v in xrange(current+1, to+1):
        fname = '_migrate_%d' % v
        with Tx(conn) as c:
            assert fname in globs, 'Migration function %d not defined.' % v
            globs[fname](c)
            c.execute("UPDATE meta SET version = %s", (v,))


def _migrate_1(c):
    c.execute('''
        CREATE DOMAIN utctime AS timestamp with time zone
                          CHECK (EXTRACT(TIMEZONE FROM VALUE) = '0')
    ''')
    c.execute('''
        CREATE TABLE meta (
            version smallint,
            last_roster_download utctime NOT NULL
        )
    ''')
    c.execute('''
        INSERT INTO meta
            (version, last_roster_download)
        VALUES (1, '0001-01-01T00:00:00Z')
    ''')


def _migrate_2(c):
    from nfldb.types import Enums, _play_categories, _player_categories

    # Create some types and common constraints.
    c.execute('''
        CREATE DOMAIN gameid AS character varying (10)
                          CHECK (char_length(VALUE) = 10)
    ''')
    c.execute('''
        CREATE DOMAIN usmallint AS smallint
                          CHECK (VALUE >= 0)
    ''')
    c.execute('''
        CREATE DOMAIN game_clock AS smallint
                          CHECK (VALUE >= 0 AND VALUE <= 900)
    ''')
    c.execute('''
        CREATE DOMAIN field_offset AS smallint
                          CHECK (VALUE >= -50 AND VALUE <= 50)
    ''')

    c.execute('''
        CREATE TYPE game_phase AS ENUM %s
    ''' % _mogrify(c, Enums.game_phase))
    c.execute('''
        CREATE TYPE season_phase AS ENUM %s
    ''' % _mogrify(c, Enums.season_phase))
    c.execute('''
        CREATE TYPE game_day AS ENUM %s
    ''' % _mogrify(c, Enums.game_day))
    c.execute('''
        CREATE TYPE player_pos AS ENUM %s
    ''' % _mogrify(c, Enums.player_pos))
    c.execute('''
        CREATE TYPE player_status AS ENUM %s
    ''' % _mogrify(c, Enums.player_status))
    c.execute('''
        CREATE TYPE game_time AS (
            phase game_phase,
            elapsed game_clock
        )
    ''')
    c.execute('''
        CREATE TYPE pos_period AS (
            elapsed usmallint
        )
    ''')
    c.execute('''
        CREATE TYPE field_pos AS (
            pos field_offset
        )
    ''')

    # Now that some types have been made, add current state to meta table.
    c.execute('''
        ALTER TABLE meta
            ADD season_type season_phase NULL,
            ADD season_year usmallint NULL
                    CHECK (season_year >= 1960 AND season_year <= 2100),
            ADD week usmallint NULL
                    CHECK (week >= 1 AND week <= 25)
    ''')

    # Create the team table and populate it.
    c.execute('''
        CREATE TABLE team (
            team_id character varying (3) NOT NULL,
            city character varying (50) NOT NULL,
            name character varying (50) NOT NULL,
            PRIMARY KEY (team_id)
        )
    ''')
    c.execute('''
        INSERT INTO team (team_id, city, name) VALUES %s
    ''' % (', '.join(_mogrify(c, team[0:3]) for team in nfldb.team.teams)))

    c.execute('''
        CREATE TABLE player (
            player_id character varying (10) NOT NULL
                CHECK (char_length(player_id) = 10),
            gsis_name character varying (75) NULL,
            full_name character varying (100) NULL,
            first_name character varying (100) NULL,
            last_name character varying (100) NULL,
            team character varying (3) NOT NULL,
            position player_pos NOT NULL,
            profile_id integer NULL,
            profile_url character varying (255) NULL,
            uniform_number usmallint NULL,
            birthdate character varying (75) NULL,
            college character varying (255) NULL,
            height character varying (100) NULL,
            weight character varying (100) NULL,
            years_pro usmallint NULL,
            status player_status NOT NULL,
            PRIMARY KEY (player_id),
            FOREIGN KEY (team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')
    c.execute('''
        CREATE TABLE game (
            gsis_id gameid NOT NULL,
            gamekey character varying (5) NULL,
            start_time utctime NOT NULL,
            week usmallint NOT NULL
                CHECK (week >= 1 AND week <= 25),
            day_of_week game_day NOT NULL,
            season_year usmallint NOT NULL
                CHECK (season_year >= 1960 AND season_year <= 2100),
            season_type season_phase NOT NULL,
            finished boolean NOT NULL,
            home_team character varying (3) NOT NULL,
            home_score usmallint NOT NULL,
            home_score_q1 usmallint NULL,
            home_score_q2 usmallint NULL,
            home_score_q3 usmallint NULL,
            home_score_q4 usmallint NULL,
            home_score_q5 usmallint NULL,
            home_turnovers usmallint NOT NULL,
            away_team character varying (3) NOT NULL,
            away_score usmallint NOT NULL,
            away_score_q1 usmallint NULL,
            away_score_q2 usmallint NULL,
            away_score_q3 usmallint NULL,
            away_score_q4 usmallint NULL,
            away_score_q5 usmallint NULL,
            away_turnovers usmallint NOT NULL,
            time_inserted utctime NOT NULL,
            time_updated utctime NOT NULL,
            PRIMARY KEY (gsis_id),
            FOREIGN KEY (home_team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE,
            FOREIGN KEY (away_team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')
    c.execute('''
        CREATE TABLE drive (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            start_field field_pos NULL,
            start_time game_time NOT NULL,
            end_field field_pos NULL,
            end_time game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            pos_time pos_period NULL,
            first_downs usmallint NOT NULL,
            result text NULL,
            penalty_yards smallint NOT NULL,
            yards_gained smallint NOT NULL,
            play_count usmallint NOT NULL,
            time_inserted utctime NOT NULL,
            time_updated utctime NOT NULL,
            PRIMARY KEY (gsis_id, drive_id),
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (pos_team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')

    # I've taken the approach of using a sparse table to represent
    # sparse play statistic data. See issue #2:
    # https://github.com/BurntSushi/nfldb/issues/2
    c.execute('''
        CREATE TABLE play (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            play_id usmallint NOT NULL,
            time game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            yardline field_pos NULL,
            down smallint NULL
                CHECK (down >= 1 AND down <= 4),
            yards_to_go smallint NULL
                CHECK (yards_to_go >= 0 AND yards_to_go <= 100),
            description text NULL,
            note text NULL,
            time_inserted utctime NOT NULL,
            time_updated utctime NOT NULL,
            %s,
            PRIMARY KEY (gsis_id, drive_id, play_id),
            FOREIGN KEY (gsis_id, drive_id)
                REFERENCES drive (gsis_id, drive_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (pos_team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''' % ', '.join([cat._sql_field for cat in _play_categories.values()]))

    c.execute('''
        CREATE TABLE play_player (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            play_id usmallint NOT NULL,
            player_id character varying (10) NOT NULL,
            team character varying (3) NOT NULL,
            %s,
            PRIMARY KEY (gsis_id, drive_id, play_id, player_id),
            FOREIGN KEY (gsis_id, drive_id, play_id)
                REFERENCES play (gsis_id, drive_id, play_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id, drive_id)
                REFERENCES drive (gsis_id, drive_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (player_id)
                REFERENCES player (player_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''' % ', '.join(cat._sql_field for cat in _player_categories.values()))


def _migrate_3(c):
    _create_stat_indexes(c)

    c.execute('''
        CREATE INDEX player_in_gsis_name ON player (gsis_name ASC);
        CREATE INDEX player_in_full_name ON player (full_name ASC);
        CREATE INDEX player_in_team ON player (team ASC);
        CREATE INDEX player_in_position ON player (position ASC);
    ''')
    c.execute('''
        CREATE INDEX game_in_gamekey ON game (gamekey ASC);
        CREATE INDEX game_in_start_time ON game (start_time ASC);
        CREATE INDEX game_in_week ON game (week ASC);
        CREATE INDEX game_in_day_of_week ON game (day_of_week ASC);
        CREATE INDEX game_in_season_year ON game (season_year ASC);
        CREATE INDEX game_in_season_type ON game (season_type ASC);
        CREATE INDEX game_in_finished ON game (finished ASC);
        CREATE INDEX game_in_home_team ON game (home_team ASC);
        CREATE INDEX game_in_away_team ON game (away_team ASC);
        CREATE INDEX game_in_home_score ON game (home_score ASC);
        CREATE INDEX game_in_away_score ON game (away_score ASC);
        CREATE INDEX game_in_home_turnovers ON game (home_turnovers ASC);
        CREATE INDEX game_in_away_turnovers ON game (away_turnovers ASC);
    ''')
    c.execute('''
        CREATE INDEX drive_in_gsis_id ON drive (gsis_id ASC);
        CREATE INDEX drive_in_drive_id ON drive (drive_id ASC);
        CREATE INDEX drive_in_start_field ON drive
            (((start_field).pos) ASC);
        CREATE INDEX drive_in_end_field ON drive
            (((end_field).pos) ASC);
        CREATE INDEX drive_in_start_time ON drive
            (((start_time).phase) ASC, ((start_time).elapsed) ASC);
        CREATE INDEX drive_in_end_time ON drive
            (((end_time).phase) ASC, ((end_time).elapsed) ASC);
        CREATE INDEX drive_in_pos_team ON drive (pos_team ASC);
        CREATE INDEX drive_in_pos_time ON drive
            (((pos_time).elapsed) DESC);
        CREATE INDEX drive_in_first_downs ON drive (first_downs DESC);
        CREATE INDEX drive_in_penalty_yards ON drive (penalty_yards DESC);
        CREATE INDEX drive_in_yards_gained ON drive (yards_gained DESC);
        CREATE INDEX drive_in_play_count ON drive (play_count DESC);
    ''')
    c.execute('''
        CREATE INDEX play_in_gsis_id ON play (gsis_id ASC);
        CREATE INDEX play_in_gsis_drive_id ON play (gsis_id ASC, drive_id ASC);
        CREATE INDEX play_in_time ON play
            (((time).phase) ASC, ((time).elapsed) ASC);
        CREATE INDEX play_in_pos_team ON play (pos_team ASC);
        CREATE INDEX play_in_yardline ON play
            (((yardline).pos) ASC);
        CREATE INDEX play_in_down ON play (down ASC);
        CREATE INDEX play_in_yards_to_go ON play (yards_to_go DESC);
    ''')
    c.execute('''
        CREATE INDEX pp_in_gsis_id ON play_player (gsis_id ASC);
        CREATE INDEX pp_in_player_id ON play_player (player_id ASC);
        CREATE INDEX pp_in_gsis_drive_id ON play_player
            (gsis_id ASC, drive_id ASC);
        CREATE INDEX pp_in_gsis_drive_play_id ON play_player
            (gsis_id ASC, drive_id ASC, play_id ASC);
        CREATE INDEX pp_in_gsis_player_id ON play_player
            (gsis_id ASC, player_id ASC);
        CREATE INDEX pp_in_team ON play_player (team ASC);
    ''')


def _migrate_4(c):
    c.execute('''
        UPDATE team SET city = 'New York' WHERE team_id IN ('NYG', 'NYJ');
        UPDATE team SET name = 'Giants' WHERE team_id = 'NYG';
        UPDATE team SET name = 'Jets' WHERE team_id = 'NYJ';
    ''')


def _migrate_5(c):
    c.execute('''
        UPDATE player SET weight = '0', height = '0'
    ''')
    c.execute('''
        ALTER TABLE player
            ALTER COLUMN height TYPE usmallint USING height::usmallint,
            ALTER COLUMN weight TYPE usmallint USING weight::usmallint;
    ''')


def _migrate_6(c):
    c.execute('''
        ALTER TABLE meta DROP CONSTRAINT meta_week_check;
        ALTER TABLE game DROP CONSTRAINT game_week_check;
        ALTER TABLE meta ADD CONSTRAINT meta_week_check
            CHECK (week >= 0 AND week <= 25);
        ALTER TABLE game ADD CONSTRAINT game_week_check
            CHECK (week >= 0 AND week <= 25);
    ''')


def _migrate_7(c):
    from nfldb.types import _player_categories

    print('''
MIGRATING DATABASE... PLEASE WAIT

THIS WILL ONLY HAPPEN ONCE.

This is currently adding a play aggregation table (a materialized view) derived
from the `play` and `play_player` tables. Depending on your machine, this
should take less than two minutes (this includes aggregating the data and
adding indexes).

This aggregation table will automatically update itself when data is added or
changed.
''', file=sys.stderr)

    c.execute('''
        CREATE TABLE agg_play (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            play_id usmallint NOT NULL,
            %s,
            PRIMARY KEY (gsis_id, drive_id, play_id),
            FOREIGN KEY (gsis_id, drive_id, play_id)
                REFERENCES play (gsis_id, drive_id, play_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id, drive_id)
                REFERENCES drive (gsis_id, drive_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES game (gsis_id)
                ON DELETE CASCADE
        )
    ''' % ', '.join(cat._sql_field for cat in _player_categories.values()))
    select = ['play.gsis_id', 'play.drive_id', 'play.play_id'] \
        + ['COALESCE(SUM(play_player.%s), 0)' % cat.category_id
           for cat in _player_categories.values()]
    c.execute('''
        INSERT INTO agg_play
        SELECT {select}
        FROM play
        LEFT JOIN play_player
        ON (play.gsis_id, play.drive_id, play.play_id)
           = (play_player.gsis_id, play_player.drive_id, play_player.play_id)
        GROUP BY play.gsis_id, play.drive_id, play.play_id
    '''.format(select=', '.join(select)))

    print('Aggregation complete. Adding indexes...', file=sys.stderr)
    c.execute('''
        CREATE INDEX agg_play_in_gsis_id
            ON agg_play (gsis_id ASC);
        CREATE INDEX agg_play_in_gsis_drive_id
            ON agg_play (gsis_id ASC, drive_id ASC);
    ''')
    for cat in _player_categories.values():
        c.execute('CREATE INDEX agg_play_in_%s ON agg_play (%s ASC)'
                  % (cat, cat))

    print('Indexing complete. Adding triggers...', file=sys.stderr)
    c.execute('''
        CREATE FUNCTION agg_play_insert() RETURNS trigger AS $$
            BEGIN
                INSERT INTO
                    agg_play (gsis_id, drive_id, play_id)
                    VALUES   (NEW.gsis_id, NEW.drive_id, NEW.play_id);
                RETURN NULL;
            END;
        $$ LANGUAGE 'plpgsql';
    ''')
    c.execute('''
        CREATE TRIGGER agg_play_sync_insert
        AFTER INSERT ON play
        FOR EACH ROW EXECUTE PROCEDURE agg_play_insert();
    ''')

    def make_sum(field):
        return 'COALESCE(SUM(play_player.{f}), 0) AS {f}'.format(f=field)
    select = [make_sum(f.category_id) for f in _player_categories.values()]
    set_columns = ['{f} = s.{f}'.format(f=f.category_id)
                   for f in _player_categories.values()]
    c.execute('''
        CREATE FUNCTION agg_play_update() RETURNS trigger AS $$
            BEGIN
                UPDATE agg_play SET {set_columns}
                FROM (
                    SELECT {select}
                    FROM play
                    LEFT JOIN play_player
                    ON (play.gsis_id, play.drive_id, play.play_id)
                       = (play_player.gsis_id, play_player.drive_id,
                          play_player.play_id)
                    WHERE (play.gsis_id, play.drive_id, play.play_id)
                          = (NEW.gsis_id, NEW.drive_id, NEW.play_id)
                ) s
                WHERE (agg_play.gsis_id, agg_play.drive_id, agg_play.play_id)
                      = (NEW.gsis_id, NEW.drive_id, NEW.play_id);
                RETURN NULL;
            END;
        $$ LANGUAGE 'plpgsql';
    '''.format(set_columns=', '.join(set_columns), select=', '.join(select)))
    c.execute('''
        CREATE TRIGGER agg_play_sync_update
        AFTER INSERT OR UPDATE ON play_player
        FOR EACH ROW EXECUTE PROCEDURE agg_play_update();
    ''')
