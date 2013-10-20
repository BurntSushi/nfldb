from __future__ import absolute_import, division, print_function

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from collections import defaultdict
import datetime
import itertools

import enum

from psycopg2.extensions import AsIs, ISQLQuote

import pytz

import nfldb.category
from nfldb.db import _upsert, now, Tx
import nfldb.team

__pdoc__ = {}


def select_columns(tabtype, prefix=None):
    """
    Return a valid SQL SELECT string for the given table type. If
    `prefix` is not `None`, then it will be used as a prefix for each
    SQL field.

    This function includes derived fields in `tabtype`.

    This should only be used if you're writing SQL queries by hand.
    """
    sql = lambda f: tabtype._as_sql(f, prefix=prefix)
    select = [sql(f) for f in tabtype._sql_columns]
    select += ['%s AS %s' % (sql(f), f) for f in tabtype._sql_derived]
    return ', '.join(select)


def _stat_categories():
    """
    Returns a `collections.OrderedDict` of all statistical categories
    available for play-by-play data.
    """
    cats = OrderedDict()
    for row in nfldb.category.categories:
        cat_type = Enums.category_scope[row[2]]
        cats[row[3]] = Category(row[3], row[0], cat_type, row[1], row[4])
    return cats


def _nflgame_start_time(schedule):
    """
    Given an entry in `nflgame.schedule`, return the start time of the
    game in UTC.
    """
    # BUG: Getting the hour here will be wrong if a game starts before Noon
    # EST. Not sure what to do about it...
    hour, minute = schedule['time'].strip().split(':')
    hour, minute = (int(hour) + 12) % 24, int(minute)
    d = datetime.datetime(schedule['year'], schedule['month'],
                          schedule['day'], hour, minute)
    return pytz.timezone('US/Eastern').localize(d).astimezone(pytz.utc)


def _nflgame_clock(clock):
    """
    Given a `nflgame.game.GameClock` object, convert and return it as
    a `nfldb.Clock` object.
    """
    phase = Enums._nflgame_game_phase[clock.quarter]
    elapsed = Clock._phase_max - ((clock._minutes * 60) + clock._seconds)
    return Clock(phase, elapsed)


def _play_time(drive, play, next_play):
    """
    Given a `nfldb.Play` object without time information and a
    `nfldb.Drive` object, returns a `nfldb.Clock` object representing
    the play's game clock. `next_play` must be a `nfldb.Play` object
    corresponding to the next play in `drive` with valid time data, or
    it can be `None` if one isn't available.

    This is used for special non-plays like "Two-Minute Warning" or
    timeouts. The source JSON data leaves the clock field NULL, but we
    want to do better than that.

    The drive is used to guess the quarter of a timeout and two-minute
    warning.
    """
    assert not play.time  # Never do this when the play has time data!

    desc = play.description.lower()
    if next_play is not None and ('timeout' in desc or 'warning' in desc):
        return next_play.time
    elif 'end game' in desc or 'end of game' in desc:
        return Clock(Enums.game_phase.Final, 0)
    elif 'end quarter' in desc:
        qtr = int(desc.strip()[12])
        if qtr == 2:
            return Clock(Enums.game_phase.Half, 0)
        elif qtr == 5:
            return Clock(Enums.game_phase.OT, Clock._phase_max)
        elif qtr == 6:
            return Clock(Enums.game_phase.OT2, Clock._phase_max)
        else:
            return Clock(Enums.game_phase['Q%d' % qtr], Clock._phase_max)
    elif 'end of quarter' in desc:
        if drive.start_time.phase is Enums.game_phase.Q2:
            return Clock(Enums.game_phase.Half, 0)
        else:
            return Clock(drive.start_time.phase, Clock._phase_max)
    elif 'end of half' in desc:
        return Clock(Enums.game_phase.Half, 0)
    return None


def _next_play_with_time(plays, play):
    """
    Returns the next `nfldb.Play` after `play` in `plays` with valid
    time data. If such a play does not exist, then `None` is returned.
    """
    get_next = False
    for p in plays:
        if get_next:
            # Don't take a play without time info.
            # e.g., Two timeouts in a row, or a two-minute warning
            # next to a timeout.
            if not p.time:
                continue
            return p
        if p.play_id == play.play_id:
            get_next = True
    return None


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


def _sum_fields(tabtype, prefix=None):
    """
    Return a valid SQL SELECT string for an aggregate query. This
    is similar to `nfldb._select_fields`, but it uses the `SUM`
    function on each field; including derived fields in `tabtype`.
    This function knows which SQL columns to aggregate by inspecting
    `nfldb.stat_categories`.

    Note that this can only be used for `nfldb.Play` and
    `nfldb.PlayPlayer` table types. Any other value will cause an
    assertion error.
    """
    assert tabtype in (Play, PlayPlayer)

    if tabtype == Play:
        fields = _play_categories.keys()
    else:
        fields = _player_categories.keys()
    fields += tabtype._sql_derived

    sql = lambda f: 'SUM(%s)' % tabtype._as_sql(f, prefix=prefix)
    select = ['%s AS %s' % (sql(f), f) for f in fields]
    return ', '.join(select)


def _total_ordering(cls):
    """Class decorator that fills in missing ordering methods"""
    # Taken from Python 2.7 stdlib to support 2.6.
    convert = {
        '__lt__': [('__gt__',
                    lambda self, other: not (self < other or self == other)),
                   ('__le__',
                    lambda self, other: self < other or self == other),
                   ('__ge__',
                    lambda self, other: not self < other)],
        '__le__': [('__ge__',
                    lambda self, other: not self <= other or self == other),
                   ('__lt__',
                    lambda self, other: self <= other and not self == other),
                   ('__gt__',
                    lambda self, other: not self <= other)],
        '__gt__': [('__lt__',
                    lambda self, other: not (self > other or self == other)),
                   ('__ge__',
                    lambda self, other: self > other or self == other),
                   ('__le__',
                    lambda self, other: not self > other)],
        '__ge__': [('__le__',
                    lambda self, other: (not self >= other) or self == other),
                   ('__gt__',
                    lambda self, other: self >= other and not self == other),
                   ('__lt__',
                    lambda self, other: not self >= other)]
    }
    roots = set(dir(cls)) & set(convert)
    if not roots:
        raise ValueError('must define at least one ordering operation: '
                         '< > <= >=')
    root = max(roots)       # prefer __lt__ to __le__ to __gt__ to __ge__
    for opname, opfunc in convert[root]:
        if opname not in roots:
            opfunc.__name__ = opname
            opfunc.__doc__ = getattr(int, opname).__doc__
            setattr(cls, opname, opfunc)
    return cls


class _Enum (enum.Enum):
    """
    Conforms to the `getquoted` interface in psycopg2. This maps enum
    types to SQL.
    """
    @staticmethod
    def _pg_cast(enum):
        """
        Returns a function to cast a SQL enum to the enumeration type
        corresponding to `enum`. Namely, `enum` should be a member of
        `nfldb.Enums`.
        """
        return lambda sqlv, _: None if not sqlv else enum[sqlv]

    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("'%s'" % self.name)
        return None

    def __str__(self):
        return self.name

    # Why can't I use the `_total_ordering` decorator on this class?

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._value_ < other._value_

    def __le__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._value_ <= other._value_

    def __gt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._value_ > other._value_

    def __ge__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._value_ >= other._value_


class Enums (object):
    """
    Enums groups all enum types used in the database schema.
    All possible values for each enum type are represented as lists.
    The ordering of each list is the same as the ordering in the
    database. In particular, this ordering specifies a total ordering
    that can be used in Python code to compare values in the same
    enumeration.
    """

    game_phase = _Enum('game_phase',
                       ['Pregame', 'Q1', 'Q2', 'Half',
                        'Q3', 'Q4', 'OT', 'OT2', 'Final'])
    """
    Represents the phase of the game. e.g., `Q1` or `Half`.
    """

    season_phase = _Enum('season_phase',
                         ['Preseason', 'Regular', 'Postseason'])
    """
    Represents one of the three phases of an NFL season: `Preseason`,
    `Regular` or `Postseason`.
    """

    game_day = _Enum('game_day',
                     ['Sunday', 'Monday', 'Tuesday', 'Wednesday',
                      'Thursday', 'Friday', 'Saturday'])
    """
    The day of the week on which a game was played. The week starts
    on `Sunday`.
    """

    player_pos = _Enum('player_pos',
                       ['C', 'CB', 'DB', 'DE', 'DL', 'DT', 'FB', 'FS', 'G',
                        'ILB', 'K', 'LB', 'LS', 'MLB', 'NT', 'OG', 'OL', 'OLB',
                        'OT', 'P', 'QB', 'RB', 'SAF', 'SS', 'T', 'TE', 'WR',
                        'UNK'])
    """
    The set of all possible player positions in abbreviated form.
    """

    player_status = _Enum('player_status',
                          ['Active', 'InjuredReserve', 'NonFootballInjury',
                           'Suspended', 'PUP', 'UnsignedDraftPick',
                           'Exempt', 'Unknown'])
    """
    The current status of a player that is actively on a
    roster. The statuses are taken from the key at the bottom of
    http://goo.gl/HHsnjD
    """

    category_scope = _Enum('category_scope', ['play', 'player'])
    """
    The scope of a particular statistic. Typically, statistics refer
    to a specific `player`, but sometimes a statistic refers to the
    totality of a play. For example, `third_down_att` is a `play`
    statistic that records third down attempts.

    Currently, `play` and `player` are the only possible values.

    Note that this type is not represented in the database schema.
    Values of this type are constructed from data in `category.py`.
    """

    _nflgame_season_phase = {
        'PRE': season_phase.Preseason,
        'REG': season_phase.Regular,
        'POST': season_phase.Postseason,
    }
    """
    Maps a season type in `nflgame` to a `nfldb.Enums.season_phase`.
    """

    _nflgame_game_phase = {
        'Pregame': game_phase.Pregame,
        'Halftime': game_phase.Half,
        'Final': game_phase.Final,
        'final': game_phase.Final,
        1: game_phase.Q1,
        2: game_phase.Q2,
        3: game_phase.Half,
        4: game_phase.Q3,
        5: game_phase.Q4,
        6: game_phase.OT,
        7: game_phase.OT2,
    }
    """
    Maps a game phase in `nflgame` to a `nfldb.Enums.game_phase`.
    """

    _nflgame_game_day = {
        'Sun': game_day.Sunday,
        'Mon': game_day.Monday,
        'Tue': game_day.Tuesday,
        'Wed': game_day.Wednesday,
        'Thu': game_day.Thursday,
        'Fri': game_day.Friday,
        'Sat': game_day.Saturday,
    }
    """
    Maps a game day of the week in `nflgame` to a
    `nfldb.Enums.game_day`.
    """

    _nflgame_player_status = {
        'ACT': player_status.Active,
        'RES': player_status.InjuredReserve,
        'NON': player_status.NonFootballInjury,
        'Suspended': player_status.Suspended,
        'PUP': player_status.PUP,
        'UDF': player_status.UnsignedDraftPick,
        'EXE': player_status.Exempt,
        # Everything else is `player_status.Unknown`
    }


class Category (object):
    """
    Represents meta data about a statistical category. This includes
    the category's scope, GSIS identifier, name and short description.
    """
    __slots__ = ['category_id', 'gsis_number', 'category_type',
                 'is_real', 'description']

    def __init__(self, category_id, gsis_number, category_type,
                 is_real, description):
        self.category_id = category_id
        """
        A unique name for this category.
        """
        self.gsis_number = gsis_number
        """
        A unique numeric identifier for this category.
        """
        self.category_type = category_type
        """
        The scope of this category represented with
        `nfldb.Enums.category_scope`.
        """
        self.is_real = is_real
        """
        Whether this statistic is a whole number of not. Currently,
        only the `defense_sk` statistic has `Category.is_real` set to
        `True`.
        """
        self.description = description
        """
        A free-form text description of this category.
        """

    @property
    def _sql_field(self):
        """
        The SQL definition of this column. Statistics are always
        NOT NULL and have a default value of `0`.

        When `Category.is_real` is `True`, then the SQL type is `real`.
        Otherwise, it's `smallint`.
        """
        typ = 'real' if self.is_real else 'smallint'
        default = '0.0' if self.is_real else '0'
        return '%s %s NOT NULL DEFAULT %s' % (self.category_id, typ, default)

    def __str__(self):
        return self.category_id

    def __eq__(self, other):
        return self.category_id == other.category_id


# We've got to put the stat category stuff here because we need the
# Enums class defined. But `Play` and `PlayPlayer` need these
# categories to fill in __slots__ in their definition too. Ugly.
stat_categories = _stat_categories()
__pdoc__['stat_categories'] = """
An ordered dictionary of every statistical category available for
play-by-play data. The keys are the category identifier (e.g.,
`passing_yds`) and the values are `nfldb.Category` objects.
"""

_play_categories = OrderedDict(
    [(n, c) for n, c in stat_categories.items()
     if c.category_type is Enums.category_scope.play])
_player_categories = OrderedDict(
    [(n, c) for n, c in stat_categories.items()
     if c.category_type is Enums.category_scope.player])

# Let's be awesome and add auto docs.
for cat in _play_categories.values():
    __pdoc__['Play.%s' % cat.category_id] = None
for cat in _player_categories.values():
    __pdoc__['PlayPlayer.%s' % cat.category_id] = None


class Team (object):
    """
    Represents information about an NFL team. This includes its
    standard three letter abbreviation, city and mascot name.
    """
    # BUG: If multiple databases are used with different team information,
    # this class won't behave correctly since it's using a global cache.

    __slots__ = ['team_id', 'city', 'name']
    __cache = defaultdict(dict)

    def __new__(cls, db, abbr):
        abbr = nfldb.team.standard_team(abbr)
        if abbr in Team.__cache:
            return Team.__cache[abbr]
        return object.__new__(cls)

    def __init__(self, db, abbr):
        """
        Introduces a new team given an abbreviation and a database
        connection. The database connection is used to retrieve other
        team information if it isn't cached already. The abbreviation
        given is passed to `nfldb.standard_team` for you.
        """
        if hasattr(self, 'team_id'):
            # Loaded from cache.
            return

        self.team_id = nfldb.team.standard_team(abbr)
        """
        The unique team identifier represented as its standard
        2 or 3 letter abbreviation.
        """
        self.city = None
        """
        The city where this team resides.
        """
        self.name = None
        """
        The full "mascot" name of this team.
        """
        if self.team_id not in Team.__cache:
            with Tx(db) as cur:
                cur.execute('SELECT * FROM team WHERE team_id = %s',
                            (self.team_id,))
                row = cur.fetchone()
                self.city = row['city']
                self.name = row['name']
            Team.__cache[self.team_id] = self

    def __str__(self):
        return '%s %s' % (self.city, self.name)

    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("'%s'" % self.team_id)
        return None


@_total_ordering
class FieldPosition (object):
    """
    Represents field position.

    The representation is an integer offset where the 50 yard line
    corresponds to '0'. Being in one's own territory corresponds to a
    negative offset while being in the opponent's territory corresponds
    to a positive offset.

    e.g., NE has the ball on the NE 45, the offset is -5.
    e.g., NE has the ball on the NYG 2, the offset is 48.

    This class also defines a total ordering on field
    positions. Namely, given f1 and f2, f1 < f2 if and only if f2
    is closer to the goal line for the team with possession of the
    football.
    """
    __slots__ = ['_offset']

    @staticmethod
    def _pg_cast(sqlv, cursor):
        if not sqlv:
            return FieldPosition(None)
        return FieldPosition(int(sqlv[1:-1]))

    @staticmethod
    def from_str(pos):
        """
        Given a string `pos` in the format `FIELD YARDLINE`, this
        returns a new `FieldPosition` object representing the yardline
        given. `FIELD` must be the string `OWN` or `OPP` and `YARDLINE`
        must be an integer in the range `[0, 50]`.

        For example, `OPP 19` corresponds to an offset of `31`
        and `OWN 5` corresponds to an offset of `-45`. Midfield can be
        expressed as either `MIDFIELD`, `OWN 50` or `OPP 50`.
        """
        if pos.upper() == 'MIDFIELD':
            return FieldPosition(0)

        field, yrdline = pos.split(' ')
        field, yrdline = field.upper(), int(yrdline)
        assert field in ('OWN', 'OPP')
        assert 0 <= yrdline <= 50

        if field == 'OWN':
            return FieldPosition(yrdline - 50)
        else:
            return FieldPosition(50 - yrdline)

    def __init__(self, offset):
        """
        Makes a new `nfldb.FieldPosition` given a field `offset`.
        `offset` must be in the integer range [-50, 50].
        """
        if offset is None:
            self._offset = None
            return
        assert -50 <= offset <= 50
        self._offset = offset

    def _add_yards(self, yards):
        """
        Returns a new `nfldb.FieldPosition` with `yards` added to this
        field position. The value of `yards` may be negative.
        """
        assert self.valid
        newoffset = max(-50, min(50, self._offset + yards))
        return FieldPosition(newoffset)

    @property
    def valid(self):
        """
        Returns `True` if and only if this field position is known and
        valid.

        Invalid field positions cannot be compared with other field
        positions.
        """
        return self._offset is not None

    def __add__(self, other):
        if isinstance(other, FieldPosition):
            toadd = other._offset
        else:
            toadd = other
        newoffset = max(-50, min(50, self._offset + toadd))
        return FieldPosition(newoffset)

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        assert self.valid and other.valid
        return self._offset < other._offset

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._offset == other._offset

    def __str__(self):
        if not self.valid:
            return 'N/A'
        elif self._offset > 0:
            return 'OPP %d' % (50 - self._offset)
        elif self._offset < 0:
            return 'OWN %d' % (50 + self._offset)
        else:
            return 'MIDFIELD'

    def __conform__(self, proto):
        if proto is ISQLQuote:
            if not self.valid:
                return AsIs("NULL")
            else:
                return AsIs("ROW(%d)::field_pos" % self._offset)
        return None


@_total_ordering
class PossessionTime (object):
    """
    Represents the possession time of a drive in seconds.

    This class defines a total ordering on possession times. Namely, p1
    < p2 if and only if p2 corresponds to a longer time of possession
    than p1.
    """
    __slots__ = ['__seconds']

    @staticmethod
    def from_str(clock_str):
        """
        Introduces a `nfldb.PossessionTime` object from a string
        formatted as clock time. For example, `2:00` corresponds to
        `120` seconds and `14:39` corresponds to `879` seconds.
        """
        minutes, seconds = map(int, clock_str.split(':', 1))
        return PossessionTime((minutes * 60) + seconds)

    @staticmethod
    def _pg_cast(sqlv, cursor):
        return PossessionTime(int(sqlv[1:-1]))

    def __init__(self, seconds):
        """
        Returns a `nfldb.PossessionTime` object given the number of
        seconds of the possession.
        """
        assert isinstance(seconds, int)
        self.__seconds = seconds

    @property
    def valid(self):
        """
        Returns `True` if and only if this possession time has a valid
        representation.

        Invalid possession times cannot be compared with other
        possession times.
        """
        return self.__seconds is not None

    @property
    def total_seconds(self):
        """
        The total seconds elapsed for this possession.
        `0` is returned if this is not a valid possession time.
        """
        return self.__seconds if self.valid else 0

    @property
    def minutes(self):
        """
        The number of whole minutes for a possession.
        e.g., `0:59` would be `0` minutes and `4:01` would be `4`
        minutes.
        `0` is returned if this is not a valid possession time.
        """
        return (self.__seconds // 60) if self.valid else 0

    @property
    def seconds(self):
        """
        The seconds portion of the possession time.
        e.g., `0:59` would be `59` seconds and `4:01` would be `1`
        second.
        `0` is returned if this is not a valid possession time.
        """
        return (self.__seconds % 60) if self.valid else 0

    def __str__(self):
        if not self.valid:
            return 'N/A'
        else:
            return '%02d:%02d' % (self.minutes, self.seconds)

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        assert self.valid and other.valid
        return self.__seconds < other.__seconds

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self.__seconds == other.__seconds

    def __conform__(self, proto):
        if proto is ISQLQuote:
            if not self.valid:
                return AsIs("NULL")
            else:
                return AsIs("ROW(%d)::pos_period" % self.__seconds)
        return None


@_total_ordering
class Clock (object):
    """
    Represents a single point in time during a game. This includes the
    quarter and the game clock time in addition to other phases of the
    game such as before the game starts, half time, overtime and when
    the game ends.

    Note that the clock time does not uniquely identify a play, since
    not all plays consume time on the clock. (e.g., A two point
    conversion.)

    This class defines a total ordering on clock times. Namely, c1 < c2
    if and only if c2 is closer to the end of the game than c1.
    """

    _nonqs = (Enums.game_phase.Pregame, Enums.game_phase.Half,
              Enums.game_phase.Final)
    """
    The phases of the game that do not have a time component.
    """

    _phase_max = 900
    """
    The maximum number of seconds in a game phase.
    """

    @staticmethod
    def from_str(phase, clock):
        """
        Introduces a new `nfldb.Clock` object given strings of the game
        phase and the clock. `phase` may be one of the values in the
        `nfldb.Enums.game_phase` enumeration. `clock` must be a clock
        string in the format `MM:SS`, e.g., `4:01` corresponds to a
        game phase with 4 minutes and 1 second remaining.
        """
        assert getattr(Enums.game_phase, phase, None) is not None, \
            '"%s" is not a valid game phase. choose one of %s' \
            % (phase, map(str, Enums.game_phase))

        minutes, seconds = map(int, clock.split(':', 1))
        elapsed = Clock._phase_max - ((minutes * 60) + seconds)
        return Clock(Enums.game_phase[phase], int(elapsed))

    @staticmethod
    def _pg_cast(sqlv, cursor):
        """
        Casts a SQL string of the form `(game_phase, elapsed)` to a
        `nfldb.Clock` object.
        """
        phase, elapsed = map(str.strip, sqlv[1:-1].split(','))
        return Clock(Enums.game_phase[phase], int(elapsed))

    def __init__(self, phase, elapsed):
        """
        Introduces a new `nfldb.Clock` object. `phase` should
        be a value from the `nfldb.Enums.game_phase` enumeration
        while `elapsed` should be the number of seconds elapsed in
        the `phase`. Note that `elapsed` is only applicable when
        `phase` is a quarter (including overtime). In all other
        cases, it will be set to `0`.

        `elapsed` should be in the range `[0, 900]` where `900`
        corresponds to the clock time `0:00` and `0` corresponds
        to the clock time `15:00`.
        """
        assert isinstance(phase, Enums.game_phase)
        assert 0 <= elapsed <= Clock._phase_max

        if phase in Clock._nonqs:
            elapsed = 0

        self.phase = phase
        """
        The phase represented by this clock object. It is guaranteed
        to have type `nfldb.Enums.game_phase`.
        """
        self.elapsed = elapsed
        """
        The number of seconds remaining in this clock's phase of the
        game. It is always set to `0` whenever the phase is not a
        quarter in the game.
        """

    @property
    def minutes(self):
        """
        If the clock has a time component, then the number of whole
        minutes **left in this phase** is returned. Otherwise, `0` is
        returned.
        """
        return (Clock._phase_max - self.elapsed) // 60

    @property
    def seconds(self):
        """
        If the clock has a time component, then the number of seconds
        **left in this phase** is returned. Otherwise, `0` is returned.
        """
        return (Clock._phase_max - self.elapsed) % 60

    def __str__(self):
        phase = self.phase
        if phase in Clock._nonqs:
            return phase.name
        else:
            return '%s %02d:%02d' % (phase.name, self.minutes, self.seconds)

    def __lt__(self, o):
        if self.__class__ is not o.__class__:
            return NotImplemented
        return (self.phase, self.elapsed) < (o.phase, o.elapsed)

    def __eq__(self, o):
        if self.__class__ is not o.__class__:
            return NotImplemented
        return self.phase == o.phase and self.elapsed == o.elapsed

    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("ROW('%s', %d)::game_time"
                        % (self.phase.name, self.elapsed))
        return None


class Player (object):
    """
    A representation of an NFL player. Note that the representation
    is inherently ephemeral; it always corresponds to the most recent
    knowledge about a player.

    Most of the fields in this object can have a `None` value. This is
    because the source JSON data only guarantees that a GSIS identifier
    and abbreviated name will be available. The rest of the player meta
    data is scraped from NFL.com's team roster pages (which invites
    infrequent uncertainty).
    """
    _table = 'player'

    _sql_columns = ['player_id', 'gsis_name', 'full_name', 'first_name',
                    'last_name', 'team', 'position', 'profile_id',
                    'profile_url', 'uniform_number', 'birthdate', 'college',
                    'height', 'weight', 'years_pro', 'status',
                    ]

    _sql_derived = []

    _sql_fields = _sql_columns + _sql_derived

    __slots__ = _sql_fields + ['_db']

    __existing = None
    """
    A cache of existing player ids in the database.
    This is only used when saving data to detect if a player
    needs to be added.
    """

    @staticmethod
    def _as_sql(field, prefix=None):
        prefix = 'player.' if prefix is None else prefix
        if field in Player._sql_columns:
            return '%s%s' % (prefix, field)
        raise AttributeError(field)

    @staticmethod
    def _from_nflgame(db, p):
        """
        Given `p` as a `nflgame.player.PlayPlayerStats` object,
        `_from_nflgame` converts `p` to a `nfldb.Player` object.
        """
        meta = ['full_name', 'first_name', 'last_name', 'team', 'position',
                'profile_id', 'profile_url', 'uniform_number', 'birthdate',
                'college', 'height', 'weight', 'years_pro', 'status']
        kwargs = {}
        if p.player is not None:
            for k in meta:
                v = getattr(p.player, k, '')
                if not v:
                    v = None
                kwargs[k] = v

            # Convert position and status values to an enumeration.
            kwargs['position'] = getattr(Enums.player_pos,
                                         kwargs['position'] or '',
                                         Enums.player_pos.UNK)

            trans = Enums._nflgame_player_status
            kwargs['status'] = trans.get(kwargs['status'] or '',
                                         Enums.player_status.Unknown)

        if kwargs.get('position', None) is None:
            kwargs['position'] = Enums.player_pos.UNK
        if kwargs.get('status', None) is None:
            kwargs['status'] = Enums.player_status.Unknown

        kwargs['team'] = nfldb.team.standard_team(kwargs.get('team', ''))
        return Player(db, p.playerid, p.name, **kwargs)

    @staticmethod
    def _from_nflgame_player(db, p):
        """
        Given `p` as a `nflgame.player.Player` object,
        `_from_nflgame_player` converts `p` to a `nfldb.Player` object.
        """
        class _Player (object):
            def __init__(self):
                self.playerid = p.player_id
                self.name = p.gsis_name
                self.player = p
        return Player._from_nflgame(db, _Player())

    @staticmethod
    def from_row(db, r):
        """
        Introduces a `nfldb.Player` object from a full SQL row from the
        `player` table.
        """
        return Player(db, r['player_id'], r['gsis_name'], r['full_name'],
                      r['first_name'], r['last_name'], r['team'],
                      r['position'], r['profile_id'], r['profile_url'],
                      r['uniform_number'], r['birthdate'], r['college'],
                      r['height'], r['weight'], r['years_pro'], r['status'])

    @staticmethod
    def from_id(db, player_id):
        """
        Given a player GSIS identifier (e.g., `00-0019596`) as a string,
        returns a `nfldb.Player` object corresponding to `player_id`.
        This function will always execute a single SQL query.

        If no corresponding player is found, `None` is returned.
        """
        with Tx(db) as cursor:
            cursor.execute('''
                SELECT %s FROM player WHERE player_id = %s
            ''' % (select_columns(Player), '%s'), (player_id,))
            if cursor.rowcount > 0:
                return Player.from_row(db, cursor.fetchone())
        return None

    def __init__(self, db, player_id, gsis_name, full_name=None,
                 first_name=None, last_name=None, team=None, position=None,
                 profile_id=None, profile_url=None, uniform_number=None,
                 birthdate=None, college=None, height=None, weight=None,
                 years_pro=None, status=None):
        """
        Introduces a new `nfldb.Player` object with the given
        attributes.

        A player object contains data known about a player as
        person. Namely, this object is not responsible for containing
        statistical data related to a player at some particular point
        in time.

        This constructor should probably not be used. Instead,
        use the more convenient constructors `Player.from_row` or
        `Player.from_id`. Alternatively, a `nfldb.Player` object can be
        obtained from the `nfldb.PlayPlayer`.`nfldb.PlayPlayer.player`
        attribute.
        """
        self._db = db

        self.player_id = player_id
        """
        The player_id linking this object `nfldb.PlayPlayer` object.

        N.B. This is the GSIS identifier string. It always has length
        10.
        """
        self.gsis_name = gsis_name
        """
        The name of a player from the source GameCenter data. This
        field is guaranteed to contain a name.
        """
        self.full_name = full_name
        """The full name of a player."""
        self.first_name = first_name
        """The first name of a player."""
        self.last_name = last_name
        """The last name of a player."""
        self.team = team
        """
        The team that the player is currently active on. If the player
        is no longer playing or is a free agent, this value may
        correspond to the `UNK` (unknown) team.
        """
        self.position = position
        """
        The current position of a player if it's available. This may
        be **not** be `None`. If the position is not known, then the
        `UNK` enum is used from `nfldb.Enums.player_pos`.
        """
        self.profile_id = profile_id
        """
        The profile identifier used on a player's canonical NFL.com
        profile page. This is used as a foreign key to connect varying
        sources of information.
        """
        self.profile_url = profile_url
        """The NFL.com profile URL for this player."""
        self.uniform_number = uniform_number
        """A player's uniform number as an integer."""
        self.birthdate = birthdate
        """A player's birth date as a free-form string."""
        self.college = college
        """A player's college as a free-form string."""
        self.height = height
        """A player's height as a free-form string."""
        self.weight = weight
        """A player's weight as a free-form string."""
        self.years_pro = years_pro
        """The number of years a player has played as an integer."""
        self.status = status
        """The current status of this player as a free-form string."""

    @property
    def _row(self):
        return _as_row(Player._sql_columns, self)

    def _save(self, cursor):
        if Player.__existing is None:
            Player.__existing = set()
            cursor.execute('SELECT player_id FROM player')
            for row in cursor.fetchall():
                Player.__existing.add(row['player_id'])
        if self.player_id not in Player.__existing:
            vals = self._row
            _upsert(cursor, 'player', vals, [vals[0]])
            Player.__existing.add(self.player_id)

    def __str__(self):
        name = self.full_name if self.full_name else self.gsis_name
        if not name:
            name = self.player_id  # Yikes.
        return '%s (%s, %s)' % (name, self.team, self.position)

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        if self.full_name and other.full_name:
            return self.full_name < other.full_name
        return self.gsis_name < other.gsis_name

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self.player_id == other.player_id


class PlayPlayer (object):
    """
    A "play player" is a statistical grouping of categories for a
    single player inside a play. For example, passing the ball to
    a receiver necessarily requires two "play players": the pass
    (by player X) and the reception (by player Y). Statistics that
    aren't included, for example, are blocks and penalties. (Although
    penalty information can be gleaned from a play's free-form
    `nfldb.Play.description` attribute.)

    Each `nfldb.PlayPlayer` object belongs to exactly one
    `nfldb.Play` and exactly one `nfldb.Player`.

    Any statistical categories not relevant to this particular play
    and player default to `0`.

    Most of the statistical fields are documented on the
    [statistical categories](http://goo.gl/wZstcY)
    wiki page. Each statistical field is an instance attribute in
    this class.
    """
    _table = 'play_player'

    _sql_columns = (['gsis_id', 'drive_id', 'play_id', 'player_id', 'team']
                    + _player_categories.keys()
                    )

    _sql_derived = ['offense_yds', 'offense_tds', 'defense_tds']

    # Define various additive combinations of fields.
    # Abuse the additive identity.
    _derived_sums = {
        'offense_yds': ['passing_yds', 'rushing_yds', 'receiving_yds',
                        'fumbles_rec_yds'],
        'offense_tds': ['passing_tds', 'receiving_tds', 'rushing_tds',
                        'fumbles_rec_tds'],
        'defense_tds': ['defense_frec_tds', 'defense_int_tds',
                        'defense_misc_tds'],
    }

    _sql_fields = _sql_columns + _sql_derived

    __slots__ = _sql_fields + ['_db', '_play', '_player', 'fields']

    # Document instance variables for derived SQL fields.
    # We hide them from the public interface, but make the doco
    # available to nfldb-mk-stat-table. Evil!
    __pdoc__['PlayPlayer.offense_yds'] = None
    __pdoc__['_PlayPlayer.offense_yds'] = \
        '''
        Corresponds to any yardage that is manufactured by the offense.
        Namely, the following fields:
        `nfldb.PlayPlayer.passing_yds`,
        `nfldb.PlayPlayer.rushing_yds`,
        `nfldb.PlayPlayer.receiving_yds` and
        `nfldb.PlayPlayer.fumbles_rec_yds`.

        This field is useful when searching for plays by net yardage
        regardless of how the yards were obtained.
        '''
    __pdoc__['PlayPlayer.offense_tds'] = None
    __pdoc__['_PlayPlayer.offense_tds'] = \
        '''
        Corresponds to any touchdown manufactured by the offense via
        a passing, reception, rush or fumble recovery.
        '''
    __pdoc__['PlayPlayer.defense_tds'] = None
    __pdoc__['_PlayPlayer.defense_tds'] = \
        '''
        Corresponds to any touchdown manufactured by the defense.
        e.g., a pick-6, fumble recovery TD, punt/FG block TD, etc.
        '''

    @staticmethod
    def _as_sql(field, prefix=None):
        prefix = 'play_player.' if prefix is None else prefix
        if field in PlayPlayer._sql_columns:
            return '%s%s' % (prefix, field)
        elif field in PlayPlayer._derived_sums:
            tosum = PlayPlayer._derived_sums[field]
            return ' + '.join('%s%s' % (prefix, f) for f in tosum)
        raise AttributeError(field)

    @staticmethod
    def _from_nflgame(db, p, pp):
        """
        Given `p` as a `nfldb.Play` object and `pp` as a
        `nflgame.player.PlayPlayerStats` object, `_from_nflgame`
        converts `pp` to a `nfldb.PlayPlayer` object.
        """
        stats = {}
        for k in _player_categories.keys() + PlayPlayer._sql_derived:
            if pp._stats.get(k, 0) != 0:
                stats[k] = pp._stats[k]

        team = nfldb.team.standard_team(pp.team)
        play_player = PlayPlayer(db, p.gsis_id, p.drive_id, p.play_id,
                                 pp.playerid, team, stats)
        play_player._play = p
        play_player._player = Player._from_nflgame(db, pp)
        return play_player

    @staticmethod
    def _from_tuple(db, t):
        """
        Introduces a new `nfldb.PlayPlayer` object from a tuple SQL
        result. This constructor exists for performance reasons and
        is only used inside the `nfldb.Query` class. In particular,
        the order of the fields in the originating SELECT query is
        significant.
        """
        cols = PlayPlayer._sql_fields
        stats = {}
        for i, v in enumerate(t[5:], 5):
            if v != 0:
                stats[cols[i]] = v
        return PlayPlayer(db, t[0], t[1], t[2], t[3], t[4], stats)

    @staticmethod
    def from_row(db, row):
        """
        Introduces a new `nfldb.PlayPlayer` object from a full SQL row
        result from the `play_player` table.
        """
        return PlayPlayer(db, row['gsis_id'], row['drive_id'],
                          row['play_id'], row['player_id'], row['team'], row)

    def __init__(self, db, gsis_id, drive_id, play_id, player_id, team,
                 stats):
        """
        Introduces a new `nfldb.PlayPlayer` object with the given
        attributes. `stats` should eb a dictionary mapping player
        statistical categories in `nfldb.stat_categories` to their
        corresponding values.

        This constructor should probably not be used. Instead, use
        `nfldb.PlayPlayer.from_row`, or more conveniently, any of
        the following `play_players` attributes:
        `nfldb.Game`.`nfldb.Game.play_players`,
        `nfldb.Drive`.`nfldb.Drive.play_players` or
        `nfldb.Play`.`nfldb.Play.play_players`.
        """
        self._play = None
        self._player = None
        self._db = db

        self.gsis_id = gsis_id
        """
        The GSIS identifier for the game that this "play player"
        belongs to.
        """
        self.drive_id = drive_id
        """
        The numeric drive identifier for this "play player". It may be
        interpreted as a sequence number.
        """
        self.play_id = play_id
        """
        The numeric play identifier for this "play player". It can
        typically be interpreted as a sequence number scoped to its
        corresponding game.
        """
        self.player_id = player_id
        """
        The player_id linking these stats to a `nfldb.Player` object.
        Use `nfldb.PlayPlayer.player` to access player meta data.

        N.B. This is the GSIS identifier string. It always has length
        10.
        """
        self.team = team
        """
        The team that this player belonged to when he recorded the
        statistics in this play.
        """
        self.fields = set()
        """The set of non-zero statistical fields set."""

        seta = setattr
        for cat in stats:
            seta(self, cat, stats[cat])
            self.fields.add(cat)

    @property
    def play(self):
        """
        The `nfldb.Play` object that this "play player" belongs
        to. The play is retrieved from the database if necessary.
        """
        if self._play is None:
            self._play = Play.from_id(self._db, self.gsis_id, self.drive_id,
                                      self.play_id)
        return self._play

    @property
    def player(self):
        """
        The `nfldb.Player` object that this "play player"
        corresponds to. The player is retrieved from the database if
        necessary.
        """
        if self._player is None:
            self._player = Player.from_id(self._db, self.player_id)
        return self._player

    @property
    def points(self):
        """
        The number of points scored in this player statistic. This
        accounts for touchdowns, extra points, two point conversions,
        field goals and safeties.
        """
        pvals = [
            ('defense_frec_tds', 6),
            ('defense_int_tds', 6),
            ('defense_misc_tds', 6),
            ('fumbles_rec_tds', 6),
            ('kicking_rec_tds', 6),
            ('kickret_tds', 6),
            ('passing_tds', 6),
            ('puntret_tds', 6),
            ('receiving_tds', 6),
            ('rushing_tds', 6),
            ('kicking_xpmade', 1),
            ('passing_twoptm', 2),
            ('receiving_twoptm', 2),
            ('rushing_twoptm', 2),
            ('kicking_fgm', 3),
            ('defense_safe', 2),
        ]
        for field, pval in pvals:
            if getattr(self, field, 0) != 0:
                return pval
        return 0.0

    @property
    def _row(self):
        return _as_row(PlayPlayer._sql_columns, self)

    def _save(self, cursor):
        vals = self._row
        _upsert(cursor, 'play_player', vals, vals[0:4])
        if self._player is not None:
            self._player._save(cursor)

    def _add(self, b):
        """
        Given two `nfldb.PlayPlayer` objects, `_add` accumulates `b`
        into `self`. Namely, no new `nfldb.PlayPlayer` objects are
        created.

        Both `self` and `b` must refer to the same player, or else an
        assertion error is raised.

        The `nfldb.aggregate` function should be used to sum collections
        of `nfldb.PlayPlayer` objects (or objects that can provide
        `nfldb.PlayPlayer` objects).
        """
        a = self
        assert a.player_id == b.player_id
        a.gsis_id = a.gsis_id if a.gsis_id == b.gsis_id else None
        a.drive_id = a.drive_id if a.drive_id == b.drive_id else None
        a.play_id = a.play_id if a.play_id == b.play_id else None
        a.team = a.team if a.team == b.team else None

        for cat in _player_categories:
            s = getattr(a, cat) + getattr(b, cat)
            if s != 0:
                setattr(a, cat, s)

        # Try to copy player meta data too.
        if a._player is None and b._player is not None:
            a._player = b._player

        # A play attached to aggregate statistics is always wrong.
        a._play = None

    def _copy(self):
        """Returns a copy of `self`."""
        stats = dict([(k, getattr(self, k, 0)) for k in _player_categories])
        pp = PlayPlayer(self._db, self.gsis_id, self.drive_id, self.play_id,
                        self.player_id, self.team, stats)
        pp._player = self._player
        pp._play = self._play
        return pp

    def __add__(self, b):
        pp = self._copy()
        pp.add(b)
        return pp

    def __str__(self):
        d = {}
        for cat in _player_categories:
            v = getattr(self, cat, 0)
            if v != 0:
                d[cat] = v
        return repr(d)

    def __getattr__(self, k):
        if k in PlayPlayer.__slots__:
            return 0
        raise AttributeError(k)


class Play (object):
    """
    Represents a single play in an NFL game. Each play has an
    assortment of meta data, possibly including the time on the clock
    in which the ball was snapped, the starting field position, the
    down, yards to go, etc. Not all plays have values for each field
    (for example, a timeout is considered a play but has no data for
    `nfldb.Play.down` or `nfldb.Play.yardline`).

    In addition to meta data describing the context of the game at the time
    the ball was snapped, plays also have statistics corresponding to the
    fields in `nfldb.stat_categories` with a `nfldb.Category.category_type`
    of `play`. For example, `third_down_att`, `fourth_down_failed` and
    `fourth_down_conv`. While the binary nature of these fields suggest
    a boolean value, they are actually integers. This makes them amenable
    to aggregation.

    Plays are also associated with player statistics or "events" that
    occurred in a play. For example, in a single play one player could
    pass the ball to another player. This is recorded as two different
    player statistics: a pass and a reception. Each one is represented
    as a `nfldb.PlayPlayer` object. Plays may have **zero or more** of
    these player statistics.

    Finally, it is important to note that there are (currently) some
    useful statistics missing. For example, there is currently no
    reliable means of determining the time on the clock when the play
    finished.  Also, there is no field describing the field position at
    the end of the play, although this may be added in the future.

    Most of the statistical fields are documented on the
    [statistical categories](http://goo.gl/YY587P)
    wiki page. Each statistical field is an instance attribute in
    this class.
    """
    _table = 'play'

    _sql_columns = (['gsis_id', 'drive_id', 'play_id', 'time', 'pos_team',
                     'yardline', 'down', 'yards_to_go', 'description', 'note',
                     'time_inserted', 'time_updated',
                     ] + _play_categories.keys()
                    )

    _sql_derived = []

    _sql_fields = _sql_columns + _sql_derived

    __slots__ = _sql_fields + ['_db', '_drive', '_play_players']

    @staticmethod
    def _as_sql(field, prefix=None):
        prefix = 'play.' if prefix is None else prefix
        if field in Play._sql_columns:
            return '%s%s' % (prefix, field)
        raise AttributeError(field)

    @staticmethod
    def _from_nflgame(db, d, p):
        """
        Given `d` as a `nfldb.Drive` object and `p` as a
        `nflgame.game.Play` object, `_from_nflgame` converts `p` to a
        `nfldb.Play` object.
        """
        stats = {}
        for k in _play_categories.keys() + Play._sql_derived:
            if p._stats.get(k, 0) != 0:
                stats[k] = p._stats[k]

        # Fix up some fields so they meet the constraints of the schema.
        # The `time` field is cleaned up afterwards in
        # `nfldb.Drive._from_nflgame`, since it needs data about surrounding
        # plays.
        time = None if not p.time else _nflgame_clock(p.time)
        yardline = FieldPosition(getattr(p.yardline, 'offset', None))
        down = p.down if 1 <= p.down <= 4 else None
        team = p.team if p.team is not None and len(p.team) > 0 else 'UNK'
        play = Play(db, d.gsis_id, d.drive_id, int(p.playid), time, team,
                    yardline, down, p.yards_togo, p.desc, p.note,
                    None, None, stats)

        play._drive = d
        play._play_players = []
        for pp in p.players:
            play._play_players.append(PlayPlayer._from_nflgame(db, play, pp))
        return play

    @staticmethod
    def _from_tuple(db, t):
        """
        Introduces a new `nfldb.Play` object from a tuple SQL
        result. This constructor exists for performance reasons and
        is only used inside the `nfldb.Query` class. In particular,
        the order of the fields in the originating SELECT query is
        significant.
        """
        cols = Play._sql_fields
        stats = {}
        for i, v in enumerate(t[12:], 12):
            if v != 0:
                stats[cols[i]] = v
        return Play(db, t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8],
                    t[9], t[10], t[11], stats)

    @staticmethod
    def from_row(db, row):
        """
        Introduces a new `nfldb.Play` object from a full SQL row result
        from the `play` table.
        """
        stats = {}
        get = row.get
        for cat in _play_categories:
            if get(cat, 0) != 0:
                stats[cat] = row[cat]
        return Play(db, row['gsis_id'], row['drive_id'], row['play_id'],
                    row['time'], row['pos_team'], row['yardline'],
                    row['down'], row['yards_to_go'], row['description'],
                    row['note'], row['time_inserted'], row['time_updated'],
                    stats)

    @staticmethod
    def from_id(db, gsis_id, drive_id, play_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string,
        an integer drive id and an integer play id, this returns a
        `nfldb.Play` object corresponding to the given identifiers.

        If no corresponding play is found, then `None` is returned.
        """
        with Tx(db) as cursor:
            q = '''
                SELECT %s FROM play WHERE (gsis_id, drive_id, play_id) = %s
            ''' % (select_columns(Play), '%s')
            cursor.execute(q, ((gsis_id, drive_id, play_id),))
            if cursor.rowcount > 0:
                return Play.from_row(db, cursor.fetchone())
        return None

    def __init__(self, db, gsis_id, drive_id, play_id, time, pos_team,
                 yardline, down, yards_to_go, description, note,
                 time_inserted, time_updated, stats):
        """
        Introduces a new `nfldb.Play` object with the given
        attributes.

        `stats` should be a dictionary of statistical play categories
        from `nfldb.stat_categories`.

        This constructor should probably not be used. Instead, use
        `nfldb.Play.from_row` or `nfldb.Play.from_id`. Alternatively,
        the `nfldb.Game`.`nfldb.Game.plays` and
        `nfldb.Drive`.`nfldb.Drive.plays` attributes can be used to get
        plays in a game or a drive, respectively.
        """
        self._drive = None
        self._play_players = None
        self._db = db

        self.gsis_id = gsis_id
        """
        The GSIS identifier for the game that this play belongs to.
        """
        self.drive_id = drive_id
        """
        The numeric drive identifier for this play. It may be
        interpreted as a sequence number.
        """
        self.play_id = play_id
        """
        The numeric play identifier for this play. It can typically
        be interpreted as a sequence number scoped to the week that
        this game was played, but it's unfortunately not completely
        consistent.
        """
        self.time = time
        """
        The time on the clock when the play started, represented with
        a `nfldb.Clock` object.
        """
        self.pos_team = pos_team
        """
        The team in possession during this play, represented as
        a team abbreviation string. Use the `nfldb.Team` constructor
        to get more information on a team.
        """
        self.yardline = yardline
        """
        The starting field position of this play represented with
        `nfldb.FieldPosition`.
        """
        self.down = down
        """
        The down on which this play begin. This may be `0` for
        "special" plays like timeouts or 2 point conversions.
        """
        self.yards_to_go = yards_to_go
        """
        The number of yards to go to get a first down or score a
        touchdown at the start of the play.
        """
        self.description = description
        """
        A (basically) free-form text description of the play. This is
        typically what you see on NFL GameCenter web pages.
        """
        self.note = note
        """
        A miscellaneous note field (as a string). Not sure what it's
        used for.
        """
        self.time_inserted = time_inserted
        """
        The date and time that this play was added to the
        database. This can be very useful when sorting plays by the
        order in which they occurred in real time. Unfortunately, such
        a sort requires that play data is updated relatively close to
        when it actually occurred.
        """
        self.time_updated = time_updated
        """The date and time that this play was last updated."""

        seta = setattr
        for cat in stats:
            seta(self, cat, stats[cat])

    @property
    def drive(self):
        """
        The `nfldb.Drive` object that contains this play. The drive is
        retrieved from the database if it hasn't been already.
        """
        if self._drive is None:
            self._drive = Drive.from_id(self._db, self.gsis_id, self.drive_id)
        return self._drive

    @property
    def play_players(self):
        """
        A list of all `nfldb.PlayPlayer`s in this play. They are
        automatically retrieved from the database if they haven't been
        already.

        If there are no players attached to this play, then an empty
        list is returned.
        """
        if self._play_players is None:
            self._play_players = []
            with Tx(self._db) as cursor:
                q = '''
                    SELECT %s FROM play_player
                    WHERE (gsis_id, drive_id, play_id) = %s
                ''' % (select_columns(PlayPlayer), '%s')
                cursor.execute(
                    q, ((self.gsis_id, self.drive_id, self.play_id),))
                for row in cursor.fetchall():
                    pp = PlayPlayer.from_row(self._db, row)
                    pp._play = self
                    self._play_players.append(pp)
        return self._play_players

    @property
    def points(self):
        """
        Returns the number of points scored in this play. See the
        documentation for `nfldb.PlayPlayer`.`nfldb.PlayPlayer.points`
        for details on what is included.
        """
        for pp in self.play_players:
            pts = pp.points
            if pts != 0:
                return pts
        return 0

    @property
    def _row(self):
        return _as_row(Play._sql_columns, self)

    def _save(self, cursor):
        vals = self._row
        _upsert(cursor, 'play', vals, vals[0:3])

        # Remove any "play players" that are stale.
        cursor.execute('''
            DELETE FROM play_player
            WHERE gsis_id = %s AND drive_id = %s AND play_id = %s
                  AND NOT (player_id = ANY (%s))
        ''', (self.gsis_id, self.drive_id, self.play_id,
              [p.player_id for p in (self._play_players or [])]))
        for pp in (self._play_players or []):
            pp._save(cursor)

    def __str__(self):
        if self.down:
            return '(%s, %s, %s, %d and %d) %s' \
                   % (self.pos_team, self.yardline, self.time.phase,
                      self.down, self.yards_to_go, self.description)
        elif self.pos_team:
            return '(%s, %s, %s) %s' \
                   % (self.pos_team, self.yardline, self.time.phase,
                      self.description)
        else:
            return '(%s) %s' % (self.time.phase, self.description)

    def __getattr__(self, k):
        if k in Play.__slots__:
            return 0
        if k in PlayPlayer.__slots__:
            for pp in self.play_players:
                v = getattr(pp, k)
                if v != 0:
                    return v
            return 0
        raise AttributeError(k)


class Drive (object):
    """
    Represents a single drive in an NFL game. Each drive has an
    assortment of meta data, possibly including the start and end
    times, the start and end field positions, the result of the drive,
    the number of penalties and first downs, and more.

    Each drive corresponds to **zero or more** plays. A drive usually
    corresponds to at least one play, but if the game is active, there
    exist valid ephemeral states where a drive has no plays.
    """
    _table = 'drive'

    _sql_columns = ['gsis_id', 'drive_id', 'start_field', 'start_time',
                    'end_field', 'end_time', 'pos_team', 'pos_time',
                    'first_downs', 'result', 'penalty_yards', 'yards_gained',
                    'play_count',
                    'time_inserted', 'time_updated',
                    ]

    _sql_derived = []

    _sql_fields = _sql_columns + _sql_derived

    __slots__ = _sql_fields + ['_db', '_game', '_plays']

    @staticmethod
    def _as_sql(field, prefix=None):
        prefix = 'drive.' if prefix is None else prefix
        if field in Drive._sql_columns:
            return '%s%s' % (prefix, field)
        raise AttributeError(field)

    @staticmethod
    def _from_nflgame(db, g, d):
        """
        Given `g` as a `nfldb.Game` object and `d` as a
        `nflgame.game.Drive` object, `_from_nflgame` converts `d` to a
        `nfldb.Drive` object.

        Generally, this function should not be used. It is called
        automatically by `nfldb.Game._from_nflgame`.
        """
        start_time = _nflgame_clock(d.time_start)
        start_field = FieldPosition(getattr(d.field_start, 'offset', None))
        end_field = FieldPosition(d.field_end.offset)
        end_time = _nflgame_clock(d.time_end)
        team = nfldb.team.standard_team(d.team)
        drive = Drive(db, g.gsis_id, d.drive_num, start_field, start_time,
                      end_field, end_time, team,
                      PossessionTime(d.pos_time.total_seconds()),
                      d.first_downs, d.result, d.penalty_yds,
                      d.total_yds, d.play_cnt, None, None)

        drive._game = g
        candidates = []
        for play in d.plays:
            candidates.append(Play._from_nflgame(db, drive, play))

        # At this point, some plays don't have valid game times. Fix it!
        # If we absolutely cannot fix it, drop the play. Maintain integrity!
        drive._plays = []
        for play in candidates:
            if play.time is None:
                play.time = _play_time(drive, play,
                                       _next_play_with_time(candidates, play))
            if play.time is not None:
                drive._plays.append(play)
        return drive

    @staticmethod
    def from_row(db, r):
        """
        Introduces a new `nfldb.Drive` object from a full SQL row
        result from the `drive` table.
        """
        return Drive(db, r['gsis_id'], r['drive_id'], r['start_field'],
                     r['start_time'], r['end_field'], r['end_time'],
                     r['pos_team'], r['pos_time'], r['first_downs'],
                     r['result'], r['penalty_yards'], r['yards_gained'],
                     r['play_count'], r['time_inserted'], r['time_updated'])

    @staticmethod
    def from_id(db, gsis_id, drive_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string
        and a integer drive id, this returns a `nfldb.Drive` object
        corresponding to the given identifiers.

        If no corresponding drive is found, then `None` is returned.
        """
        with Tx(db) as cursor:
            cursor.execute('''
                SELECT %s FROM drive WHERE (gsis_id, drive_id) = %s
            ''' % (select_columns(Drive), '%s'), ((gsis_id, drive_id),))
            if cursor.rowcount > 0:
                return Drive.from_row(db, cursor.fetchone())
        return None

    def __init__(self, db, gsis_id, drive_id, start_field, start_time,
                 end_field, end_time, pos_team, pos_time,
                 first_downs, result, penalty_yards, yards_gained, play_count,
                 time_inserted, time_updated):
        """
        Introduces a new `nfldb.Drive` object with the given attributes.

        This constructor should probably not be used. Instead, use
        `nfldb.Drive.from_row` or `nfldb.Drive.from_id`. Alternatively,
        the `nfldb.Game`.`nfldb.Game.drives` attribute contains all
        drives in the corresponding game.
        """
        self._game = None
        self._plays = None
        self._db = db

        self.gsis_id = gsis_id
        """
        The GSIS identifier for the game that this drive belongs to.
        """
        self.drive_id = drive_id
        """
        The numeric drive identifier for this drive. It may be
        interpreted as a sequence number.
        """
        self.start_field = start_field
        """
        The starting field position of this drive represented
        with `nfldb.FieldPosition`.
        """
        self.start_time = start_time
        """
        The starting clock time of this drive, represented with
        `nfldb.Clock`.
        """
        self.end_field = end_field
        """
        The ending field position of this drive represented with
        `nfldb.FieldPosition`.
        """
        self.end_time = end_time
        """
        The ending clock time of this drive, represented with
        `nfldb.Clock`.
        """
        self.pos_team = pos_team
        """
        The team in possession during this drive, represented as
        a team abbreviation string. Use the `nfldb.Team` constructor
        to get more information on a team.
        """
        self.pos_time = pos_time
        """
        The possession time of this drive, represented with
        `nfldb.PossessionTime`.
        """
        self.first_downs = first_downs
        """
        The number of first downs that occurred in this drive.
        """
        self.result = result
        """
        A freeform text field straight from NFL's GameCenter data that
        sometimes contains the result of a drive (e.g., `Touchdown`).
        """
        self.penalty_yards = penalty_yards
        """
        The number of yards lost or gained from penalties in this
        drive.
        """
        self.yards_gained = yards_gained
        """
        The total number of yards gained or lost in this drive.
        """
        self.play_count = play_count
        """
        The total number of plays executed by the offense in this
        drive.
        """
        self.time_inserted = time_inserted
        """The date and time that this drive was added."""
        self.time_updated = time_updated
        """The date and time that this drive was last updated."""

    @property
    def game(self):
        """
        Returns the `nfldb.Game` object that contains this drive. The
        game is retrieved from the database if it hasn't been already.
        """
        if self._game is None:
            return Game.from_id(self.gsis_id)
        return self._game

    @property
    def plays(self):
        """
        A list of all `nfldb.Play`s in this drive. They are
        automatically retrieved from the database if they haven't been
        already.

        If there are no plays in the drive, then an empty list is
        returned.
        """
        if self._plays is None:
            self._plays = []
            with Tx(self._db) as cursor:
                q = '''
                    SELECT %s FROM play WHERE (gsis_id, drive_id) = %s
                    ORDER BY time ASC, play_id ASC
                ''' % (select_columns(Play), '%s')
                cursor.execute(q, ((self.gsis_id, self.drive_id),))
                for row in cursor.fetchall():
                    p = Play.from_row(self._db, row)
                    p._drive = self
                    self._plays.append(p)
        return self._plays

    @property
    def play_players(self):
        """
        A list of `nfldb.PlayPlayer` objects in this drive. Data is
        retrieved from the database if it hasn't been already.
        """
        pps = []
        for play in self.plays:
            for pp in play.play_players:
                pps.append(pp)
        return pps

    @property
    def _row(self):
        return _as_row(Drive._sql_columns, self)

    def _save(self, cursor):
        vals = self._row
        _upsert(cursor, 'drive', vals, vals[0:2])

        if not self._plays:
            return

        # Remove any plays that are stale.
        cursor.execute('''
            DELETE FROM play
            WHERE gsis_id = %s AND drive_id = %s AND NOT (play_id = ANY (%s))
        ''', (self.gsis_id, self.drive_id, [p.play_id for p in self._plays]))
        for play in (self._plays or []):
            play._save(cursor)

    def __str__(self):
        s = '[%-12s] %-3s from %-6s to %-6s '
        s += '(lasted %s - %s to %s)'
        return s % (
            self.result, self.pos_team, self.start_field, self.end_field,
            self.pos_time, self.start_time, self.end_time,
        )


class Game (object):
    """
    Represents a single NFL game in the preseason, regular season or
    post season. Each game has an assortment of meta data, including
    a quarterly breakdown of scores, turnovers, the time the game
    started, the season week the game occurred in, and more.

    Each game corresponds to **zero or more** drives. A game usually
    corresponds to at least one drive, but if the game is active, there
    exist valid ephemeral states where a game has no drives.
    """
    _table = 'game'

    _sql_columns = ['gsis_id', 'gamekey', 'start_time', 'week', 'day_of_week',
                    'season_year', 'season_type', 'finished',
                    'home_team', 'home_score', 'home_score_q1',
                    'home_score_q2', 'home_score_q3', 'home_score_q4',
                    'home_score_q5', 'home_turnovers',
                    'away_team', 'away_score', 'away_score_q1',
                    'away_score_q2', 'away_score_q3', 'away_score_q4',
                    'away_score_q5', 'away_turnovers',
                    'time_inserted', 'time_updated']

    _sql_derived = ['winner', 'loser']

    _sql_fields = _sql_columns + _sql_derived

    __slots__ = _sql_fields + ['_db', '_drives']

    # Document instance variables for derived SQL fields.
    __pdoc__['Game.winner'] = '''The winner of this game.'''
    __pdoc__['Game.loser'] = '''The loser of this game.'''

    @staticmethod
    def _as_sql(field, prefix=None):
        prefix = 'game.' if prefix is None else prefix
        if field in Game._sql_columns:
            return '%s%s' % (prefix, field)
        elif field == 'winner':
            return '''
                (CASE WHEN {prefix}home_score > {prefix}away_score
                    THEN {prefix}home_team
                    ELSE {prefix}away_team
                 END)'''.format(prefix=prefix)
        elif field == 'loser':
            return '''
                (CASE WHEN {prefix}home_score < {prefix}away_score
                    THEN {prefix}home_team
                    ELSE {prefix}away_team
                 END)'''.format(prefix=prefix)
        raise AttributeError(field)

    @staticmethod
    def _from_nflgame(db, g):
        """
        Converts a `nflgame.game.Game` object to a `nfldb.Game`
        object.

        `db` should be a psycopg2 connection returned by
        `nfldb.connect`.
        """
        home_team = nfldb.team.standard_team(g.home)
        away_team = nfldb.team.standard_team(g.away)
        season_type = Enums._nflgame_season_phase[g.schedule['season_type']]
        day_of_week = Enums._nflgame_game_day[g.schedule['wday']]
        start_time = _nflgame_start_time(g.schedule)
        finished = g.game_over()

        # If it's been 8 hours since game start, we always conclude finished!
        if (now() - start_time).total_seconds() >= (60 * 60 * 8):
            finished = True

        # The season year should always be the same for every game in the
        # season. e.g., games played in jan-feb of 2013 are in season 2012.
        season_year = g.schedule['year']
        if int(g.eid[4:6]) <= 3:
            season_year -= 1
        game = Game(db, g.eid, g.gamekey, start_time, g.schedule['week'],
                    day_of_week, season_year, season_type, finished,
                    home_team, g.score_home, g.score_home_q1,
                    g.score_home_q2, g.score_home_q3, g.score_home_q4,
                    g.score_home_q5, int(g.data['home']['to']),
                    away_team, g.score_away, g.score_away_q1,
                    g.score_away_q2, g.score_away_q3, g.score_away_q4,
                    g.score_away_q5, int(g.data['away']['to']),
                    None, None)

        game._drives = []
        for drive in g.drives:
            if not hasattr(drive, 'game'):
                continue
            game._drives.append(Drive._from_nflgame(db, game, drive))
        return game

    @staticmethod
    def _from_schedule(db, s):
        """
        Converts a schedule dictionary from the `nflgame.schedule`
        module to a bare-bones `nfldb.Game` object.
        """
        # This is about as evil as it gets. Duck typing to the MAX!
        class _Game (object):
            def __init__(self):
                self.schedule = s
                self.home, self.away = s['home'], s['away']
                self.eid = s['eid']
                self.gamekey = s['gamekey']
                self.drives = []
                self.game_over = lambda: False

                zeroes = ['score_%s', 'score_%s_q1', 'score_%s_q2',
                          'score_%s_q3', 'score_%s_q4', 'score_%s_q5']
                for which, k in itertools.product(('home', 'away'), zeroes):
                    setattr(self, k % which, 0)
                self.data = {'home': {'to': 0}, 'away': {'to': 0}}
        return Game._from_nflgame(db, _Game())

    @staticmethod
    def from_row(db, row):
        """
        Introduces a new `nfldb.Game` object from a full SQL row
        result from the `game` table.
        """
        return Game(db, **row)

    @staticmethod
    def from_id(db, gsis_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string,
        returns a `nfldb.Game` object corresponding to `gsis_id`.

        If no corresponding game is found, `None` is returned.
        """
        with Tx(db) as cursor:
            cursor.execute('''
                SELECT %s FROM game WHERE gsis_id = %s
                ORDER BY gsis_id ASC
            ''' % (select_columns(Game), '%s'), (gsis_id,))
            if cursor.rowcount > 0:
                return Game.from_row(db, cursor.fetchone())
        return None

    def __init__(self, db, gsis_id, gamekey, start_time, week, day_of_week,
                 season_year, season_type, finished,
                 home_team, home_score, home_score_q1, home_score_q2,
                 home_score_q3, home_score_q4, home_score_q5, home_turnovers,
                 away_team, away_score, away_score_q1, away_score_q2,
                 away_score_q3, away_score_q4, away_score_q5, away_turnovers,
                 time_inserted, time_updated, loser=None, winner=None):
        """
        Introduces a new `nfldb.Drive` object with the given attributes.

        This constructor should probably not be used. Instead, use
        `nfldb.Game.from_row` or `nfldb.Game.from_id`.
        """
        self._drives = None

        self._db = db
        """
        The psycopg2 database connection.
        """
        self.gsis_id = gsis_id
        """
        The NFL GameCenter id of the game. It is a string
        with 10 characters. The first 8 correspond to the date of the
        game, while the last 2 correspond to an id unique to the week that
        the game was played.
        """
        self.gamekey = gamekey
        """
        Another unique identifier for a game used by the
        NFL. It is a sequence number represented as a 5 character string.
        The gamekey is specifically used to tie games to other resources,
        like the NFL's content delivery network.
        """
        self.start_time = start_time
        """
        A Python datetime object corresponding to the start time of
        the game. The timezone of this value will be equivalent to the
        timezone specified by `nfldb.set_timezone` (which is by default
        set to the value specified in the configuration file).
        """
        self.week = week
        """
        The week number of this game. It is always relative
        to the phase of the season. Namely, the first week of preseason
        is 1 and so is the first week of the regular season.
        """
        self.day_of_week = day_of_week
        """
        The day of the week this game was played on.
        Possible values correspond to the `nfldb.Enums.game_day` enum.
        """
        self.season_year = season_year
        """
        The year of the season of this game. This
        does not necessarily match the year that the game was played. For
        example, games played in January 2013 are in season 2012.
        """
        self.season_type = season_type
        """
        The phase of the season. e.g., `Preseason`,
        `Regular season` or `Postseason`. All valid values correspond
        to the `nfldb.Enums.season_phase`.
        """
        self.finished = finished
        """
        A boolean that is `True` if and only if the game has finished.
        """
        self.home_team = home_team
        """
        The team abbreviation for the home team. Use the `nfldb.Team`
        constructor to get more information on a team.
        """
        self.home_score = home_score
        """The current total score for the home team."""
        self.home_score_q1 = home_score_q1
        """The 1st quarter score for the home team."""
        self.home_score_q2 = home_score_q2
        """The 2nd quarter score for the home team."""
        self.home_score_q3 = home_score_q3
        """The 3rd quarter score for the home team."""
        self.home_score_q4 = home_score_q4
        """The 4th quarter score for the home team."""
        self.home_score_q5 = home_score_q5
        """The OT quarter score for the home team."""
        self.home_turnovers = home_turnovers
        """Total turnovers for the home team."""
        self.away_team = away_team
        """
        The team abbreviation for the away team. Use the `nfldb.Team`
        constructor to get more information on a team.
        """
        self.away_score = away_score
        """The current total score for the away team."""
        self.away_score_q1 = away_score_q1
        """The 1st quarter score for the away team."""
        self.away_score_q2 = away_score_q2
        """The 2nd quarter score for the away team."""
        self.away_score_q3 = away_score_q3
        """The 3rd quarter score for the away team."""
        self.away_score_q4 = away_score_q4
        """The 4th quarter score for the away team."""
        self.away_score_q5 = away_score_q5
        """The OT quarter score for the away team."""
        self.away_turnovers = away_turnovers
        """Total turnovers for the away team."""
        self.time_inserted = time_inserted
        """The date and time that this game was added."""
        self.time_updated = time_updated
        """The date and time that this game was last updated."""

    @property
    def is_playing(self):
        """
        Returns `True` is the game is currently being played and
        `False` otherwise.

        A game is being played if it is not finished and if the current
        time proceeds the game's start time.
        """
        return not self.finished and now() >= self.start_time

    @property
    def drives(self):
        """
        A list of `nfldb.Drive`s for this game. They are automatically
        loaded from the database if they haven't been already.

        If there are no drives found in the game, then an empty list
        is returned.
        """
        if self._drives is None:
            self._drives = []
            with Tx(self._db) as cursor:
                cursor.execute('''
                    SELECT %s FROM drive WHERE gsis_id = %s
                    ORDER BY start_time ASC, drive_id ASC
                ''' % (select_columns(Drive), '%s'), (self.gsis_id,))
                for row in cursor.fetchall():
                    d = Drive.from_row(self._db, row)
                    d._game = self
                    self._drives.append(d)
        return self._drives

    @property
    def plays(self):
        """
        A list of `nfldb.Play` objects in this game. Data is retrieved
        from the database if it hasn't been already.
        """
        plays = []
        for drive in self.drives:
            for play in drive.plays:
                plays.append(play)
        return plays

    @property
    def play_players(self):
        """
        A list of `nfldb.PlayPlayer` objects in this game. Data is
        retrieved from the database if it hasn't been already.
        """
        pps = []
        for play in self.plays:
            for pp in play.play_players:
                pps.append(pp)
        return pps

    @property
    def players(self):
        """
        A list of tuples of player data. The first element is the team
        the player was on during the game and the second element is a
        `nfldb.Player` object corresponding to that player's meta data
        (including the team he's currently on). The list is returned
        without duplicates and sorted by team and player name.
        """
        pset = set()
        players = []
        for pp in self.play_players:
            if pp.player_id not in pset:
                players.append((pp.team, pp.player))
                pset.add(pp.player_id)
        return sorted(players)

    @property
    def _row(self):
        return _as_row(Game._sql_columns, self)

    def _save(self, cursor):
        vals = self._row
        _upsert(cursor, 'game', vals, [vals[0]])

        if not self._drives:
            return

        # Remove any drives that are stale.
        cursor.execute('''
            DELETE FROM drive
            WHERE gsis_id = %s AND NOT (drive_id = ANY (%s))
        ''', (self.gsis_id, [d.drive_id for d in self._drives]))
        for drive in (self._drives or []):
            drive._save(cursor)

    def __str__(self):
        return '%s %d week %d on %s at %s, %s (%d) at %s (%d)' \
               % (self.season_type, self.season_year, self.week,
                  self.start_time.strftime('%m/%d'),
                  self.start_time.strftime('%I:%M%p'),
                  self.away_team, self.away_score,
                  self.home_team, self.home_score)
