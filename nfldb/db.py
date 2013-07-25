import re
import sys

import psycopg2
from psycopg2.extras import NamedTupleCursor

import toml


# Documented in __init__.py to appease epydoc.
api_version = 1


def connect(database=None, user=None, password=None, host=None, port=None):
    """
    Returns a pgsql connection from the psycopg2 library. If
    database is None, then connect will look for a configuration
    file at $XDG_CONFIG_HOME/nfldb/config.toml with the database
    connection information. Otherwise, the connection will use
    the parameters given.

    This function will also compare the current schema version of
    the database against the library version and assert that they
    are equivalent. If the schema library version is less than the
    the library version, then the schema will be automatically
    upgraded. If the schema version is newer than the library
    version, then this function will raise an assertion error.
    An assertion error will also be raised if the schema version
    is 0 and the database is not empty.
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
    conn = psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port,
                            cursor_factory=NamedTupleCursor)

    # Start the migration. Make sure if this is the initial setup that
    # the DB is empty.
    schema_version = version(conn)
    assert schema_version <= api_version, \
        'Library with version %d is older than the schema with version %d' \
        % (api_version, schema_version)
    assert schema_version > 0 or (schema_version == 0 and _is_empty(conn)), \
        'Schema has version 0 but is not empty.'
    _migrate(conn, api_version)

    return conn


def version(conn):
    """
    Returns the schema version of the given database. If the version
    is not stored in the database, then 0 is returned.
    """
    with _tx(conn) as c:
        try:
            c.execute('SELECT value FROM meta WHERE name = %s', ['version'])
        except psycopg2.ProgrammingError:
            conn.rollback()
            return 0
        if c.rowcount == 0:
            return 0
        return int(c.fetchone().value)


def _db_name(conn):
    m = re.search('dbname=(\S+)', conn.dsn)
    return m.group(1)


def _is_empty(conn):
    """
    Returns True if and only if there are no tables in the given
    database.
    """
    with _tx(conn) as c:
        c.execute('''
            SELECT COUNT(*) AS count FROM information_schema.tables
            WHERE table_catalog = %s AND table_schema = 'public'
        ''', [_db_name(conn)])
        if c.fetchone().count == 0:
            return True
        return False


class _tx (object):
    def __init__(self, psycho_conn):
        self.conn = psycho_conn
        self.cursor = None

    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, typ, value, traceback):
        if not self.cursor.closed:
            self.cursor.close()
        if typ is not None:
            self.conn.rollback()
            return False
        else:
            self.conn.commit()
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
    current = version(conn)
    assert current <= to

    globs = globals()
    for v in xrange(current+1, to+1):
        fname = '_migrate_%d' % v
        with _tx(conn) as c:
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
