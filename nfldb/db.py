import csv
import os.path as path
import re
import sys

import psycopg2
from psycopg2.extras import NamedTupleCursor

import toml

import nflgame

__pdoc__ = {}

api_version = 2
__pdoc__['api_version'] = \
    """
    The schema version that this library corresponds to. When the schema
    version of the database is less than this value, `nfldb.connect` will
    automatically update the schema to the latest version before doing
    anything else.
    """


def connect(database=None, user=None, password=None, host=None, port=None,
            timezone=None):
    """
    Returns a `psycopg2._psycopg.connection` object from the
    `psycopg2.connect`. If database is `None`, then `connect` will look
    for a configuration file at `$XDG_CONFIG_HOME/nfldb/config.toml`
    with the database connection information. Otherwise, the connection
    will use the parameters given.

    This function will also compare the current schema version of the
    database against the API version `nfldb.api_version` and assert
    that they are equivalent. If the schema library version is less
    than the the API version, then the schema will be automatically
    upgraded. If the schema version is newer than the library version,
    then this function will raise an assertion error.  An assertion
    error will also be raised if the schema version is 0 and the
    database is not empty.

    N.B. The `timezone` parameter should be set to a value that
    PostgreSQL will accept. Select from the `pg_timezone_names` view
    to get a list of valid time zones. (Like the other parameters,
    when `database` is `None`, the `timezone` parameter is filled from
    `config.toml`.)
    """
    if database is None:
        try:
            conf = toml.loads(open('config.toml').read())
        except:
            print >> sys.stderr, "Invalid configuration file format."
            sys.exit(1)
        database = conf['pgsql'].get('database', None)
        user = conf['pgsql'].get('user', None)
        password = conf['pgsql'].get('password', None)
        host = conf['pgsql'].get('host', None)
        port = conf['pgsql'].get('port', None)
        timezone = conf.get('timezone', 'US/Eastern')
    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port,
                            cursor_factory=NamedTupleCursor)
    set_timezone(conn, timezone)

    # Start the migration. Make sure if this is the initial setup that
    # the DB is empty.
    sversion = schema_version(conn)
    assert sversion <= api_version, \
        'Library with version %d is older than the schema with version %d' \
        % (api_version, sversion)
    assert sversion > 0 or (sversion == 0 and _is_empty(conn)), \
        'Schema has version 0 but is not empty.'
    _migrate(conn, api_version)

    return conn


def schema_version(conn):
    """
    Returns the schema version of the given database. If the version
    is not stored in the database, then `0` is returned.
    """
    with Tx(conn) as c:
        try:
            c.execute('SELECT value FROM meta WHERE name = %s', ['version'])
        except psycopg2.ProgrammingError:
            conn.rollback()
            return 0
        if c.rowcount == 0:
            return 0
        return int(c.fetchone().value)


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
        if c.fetchone().count == 0:
            return True
    return False


def _mogrify(cursor, xs):
    """Shortcut for mogrifying a list as if it were a tuple."""
    return cursor.mogrify('%s', (tuple(xs),))


class Tx (object):
    """
    Tx is a `with` compatible class that abstracts a transaction given
    a connection. If an exception occurs inside the `with` block, then
    rollback is automatically called. Otherwise, upon exit of the with
    block, commit is called.

    Use it like so:

        #!python
        with Tx(conn) as cursor:
            ...

    Which is meant to be equivalent to the following:

        #!python
        with conn:
            with conn.cursor() as curs:
                ...
    """
    def __init__(self, psycho_conn):
        self.__conn = psycho_conn
        self.__cursor = None

    def __enter__(self):
        self.__cursor = self.__conn.cursor()
        return self.__cursor

    def __exit__(self, typ, value, traceback):
        if not self.__cursor.closed:
            self.__cursor.close()
        if typ is not None:
            self.__conn.rollback()
            return False
        else:
            self.__conn.commit()
            return True


# What follows are the migration functions. They follow the naming
# convention "_migrate_{VERSION}" where VERSION is an integer that
# corresponds to the version that the schema will be after the
# migration function runs. Each migration function is only responsible
# for running the queries required to update schema. It does not
# need to update the schema version.
#
# The migration functions should accept a cursor as a parameter,
# which are created in the higher-order _migrate. In particular,
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
            c.execute("UPDATE meta SET value = %s WHERE name = 'version'", [v])


def _migrate_1(c):
    c.execute('''
        CREATE TABLE meta (
            name varchar (255) PRIMARY KEY,
            value varchar (1000) NOT NULL
        )
    ''')
    c.execute("INSERT INTO meta (name, value) VALUES ('version', '1')")


def _migrate_2(c):
    from nfldb.types import Enums

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
        CREATE DOMAIN fieldpos AS smallint
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
        CREATE TYPE playerpos AS ENUM %s
    ''' % _mogrify(c, Enums.playerpos))
    c.execute('''
        CREATE TYPE category_scope AS ENUM %s
    ''' % _mogrify(c, Enums.category_scope))
    c.execute('''
        CREATE TYPE game_time AS (
            phase game_phase,
            elapsed game_clock
        )
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
    ''' % (', '.join(_mogrify(c, team[0:3]) for team in nflgame.teams)))

    # Create the stat category table and populate it.
    c.execute('''
        CREATE TABLE category (
            category_id character varying (50) NOT NULL,
            gsis_number usmallint NOT NULL,
            category_type category_scope NOT NULL,
            description text,
            PRIMARY KEY (category_id)
        )
    ''')
    with open(path.join(path.split(__file__)[0], 'data-dictionary.csv')) as f:
        c.execute('''
            INSERT INTO category
                (gsis_number, category_type, category_id, description)
            VALUES %s
        ''' % (', '.join(_mogrify(c, row)
               for row in csv.reader(f, delimiter='\t'))))

    # Create the rest of the schema.
    c.execute('''
        CREATE TABLE player (
            player_id serial NOT NULL,
            player_gsis_id character varying (10) NOT NULL
                CHECK (char_length(player_gsis_id) = 10),
            gsis_name character varying (75) NOT NULL,
            full_name character varying (75) NULL,
            current_team character varying (3) NOT NULL,
            position playerpos NOT NULL,
            PRIMARY KEY (player_id),
            FOREIGN KEY (current_team)
                REFERENCES team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')
    c.execute('''
        CREATE TABLE game (
            gsis_id gameid NOT NULL,
            gamekey character varying (5) NULL,
            start_time timestamp with time zone NOT NULL
                CHECK (EXTRACT(TIMEZONE FROM start_time) = '0'),
            week usmallint NOT NULL
                CHECK (week >= 1 AND week <= 25),
            day_of_week game_day NOT NULL,
            season_year usmallint NOT NULL
                CHECK (season_year >= 1960 AND season_year <= 2100),
            season_type season_phase NOT NULL,
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
            start_field fieldpos NULL,
            start_time game_time NOT NULL,
            end_field fieldpos NULL,
            end_time game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            pos_time usmallint NULL,
            first_downs usmallint NOT NULL,
            result text NULL,
            penalty_yards smallint NOT NULL,
            yards_gained smallint NOT NULL,
            play_count usmallint NOT NULL,
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
    c.execute('''
        CREATE TABLE play (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            play_id usmallint NOT NULL,
            time game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            yardline fieldpos NULL,
            down smallint NULL
                CHECK (down >= 1 AND down <= 4),
            yards_to_go smallint NULL
                CHECK (yards_to_go >= 0 AND yards_to_go <= 100),
            description text NULL,
            note text NULL,
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
    ''')
    c.execute('''
        CREATE TABLE stat (
            gsis_id gameid NOT NULL,
            drive_id usmallint NOT NULL,
            play_id usmallint NOT NULL,
            player_id integer NOT NULL,
            category_id character varying (50) NOT NULL,
            value real NOT NULL,
            PRIMARY KEY (gsis_id, drive_id, play_id, player_id, category_id),
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
            FOREIGN KEY (category_id)
                REFERENCES category (category_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''')

    # Now create all of the indexes.
    c.execute('''
        CREATE INDEX player_in_player_gsis_id ON player (player_gsis_id ASC);
        CREATE INDEX player_in_gsis_name ON player (gsis_name ASC);
        CREATE INDEX player_in_full_name ON player (full_name ASC);
        CREATE INDEX player_in_current_team ON player (current_team ASC);
        CREATE INDEX player_in_position ON player (position ASC);
    ''')
    c.execute('''
        CREATE INDEX game_in_gamekey ON game (gamekey ASC);
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
        CREATE INDEX drive_in_start_field ON drive (start_field ASC);
        CREATE INDEX drive_in_end_field ON drive (end_field ASC);
        CREATE INDEX drive_in_start_time ON drive
            (((start_time).phase) ASC, ((start_time).elapsed) ASC);
        CREATE INDEX drive_in_end_time ON drive
            (((end_time).phase) ASC, ((end_time).elapsed) ASC);
        CREATE INDEX drive_in_pos_team ON drive (pos_team ASC);
        CREATE INDEX drive_in_pos_time ON drive (pos_time DESC);
        CREATE INDEX drive_in_first_downs ON drive (first_downs DESC);
        CREATE INDEX drive_in_penalty_yards ON drive (penalty_yards DESC);
        CREATE INDEX drive_in_yards_gained ON drive (yards_gained DESC);
        CREATE INDEX drive_in_play_count ON drive (play_count DESC);
    ''')
    c.execute('''
        CREATE INDEX play_in_gsis_id ON play (gsis_id ASC);
        CREATE INDEX play_in_drive_id ON play (drive_id ASC);
        CREATE INDEX play_in_time ON play
            (((time).phase) ASC, ((time).elapsed) ASC);
        CREATE INDEX play_in_yardline ON play (yardline ASC);
        CREATE INDEX play_in_down ON play (down ASC);
        CREATE INDEX play_in_yards_to_go ON play (yards_to_go DESC);
    ''')
    c.execute('''
        CREATE INDEX stat_in_player_id ON stat (player_id ASC);
        CREATE INDEX stat_in_category_id ON stat (category_id ASC);
    ''')
