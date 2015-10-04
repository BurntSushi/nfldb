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
from nfldb.db import now, Tx
import nfldb.sql as sql
import nfldb.team

__pdoc__ = {}


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
    # Hack to get around ambiugous times for weird London games.
    if schedule['eid'] == '2015100400':
        d = datetime.datetime(2015, 10, 4, 9, 30)
        return pytz.timezone('US/Eastern').localize(d).astimezone(pytz.utc)

    # Year is always the season, so we bump it if the month is Jan-March.
    year, month, day = schedule['year'], schedule['month'], schedule['day']
    if 1 <= schedule['month'] <= 3:
        year += 1

    # BUG: Getting the hour here will be wrong if a game starts before Noon
    # EST. Not sure what to do about it...
    hour, minute = schedule['time'].strip().split(':')
    minute = int(minute)
    if hour == '12':
        hour = 12
    else:
        hour = (int(hour) + 12) % 24
    d = datetime.datetime(year, month, day, hour, minute)
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


def _next_play_with(plays, play, pred):
    """
    Returns the next `nfldb.Play` after `play` in `plays` where `pred`
    returns True (given a `nfldb.Play` object).  If such a play does
    not exist, then `None` is returned.
    """
    get_next = False
    for p in plays:
        if get_next:
            # Don't take a play that isn't satisfied.
            # e.g. for time, Two timeouts in a row, or a two-minute warning
            # next to a timeout.
            if not pred(p):
                continue
            return p
        if p.play_id == play.play_id:
            get_next = True
    return None


def _fill(db, fill_with, to_fill, attr):
    """
    Fills a list of entities `to_fill` with the entity `fill_with`.
    An instance of the appropriate `fill_with` entity is assigned
    to the `attr` of `to_fill`.
    """
    pk = fill_with._sql_tables['primary']
    def pkval(entobj):
        return tuple(getattr(entobj, k) for k in pk)

    import nfldb.query
    ids = list(set(pkval(obj) for obj in to_fill))
    if len(ids) == 0:
        return
    objs = nfldb.query._entities_by_ids(db, fill_with, *ids)
    byid = dict([(pkval(obj), obj) for obj in objs])
    for obj in to_fill:
        setattr(obj, attr, byid[pkval(obj)])


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
    types to SQL and back.
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

    Note that this type is not represented directly in the database
    schema. Values of this type are constructed from data in
    `category.py`.
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
        Whether this statistic is a real number or not. Currently,
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

# Don't document these fields because there are too many.
# Instead, the API docs will include a link to a Wiki page with a table
# of stat categories.
for cat in _play_categories.values():
    __pdoc__['Play.%s' % cat.category_id] = None
for cat in _player_categories.values():
    __pdoc__['Play.%s' % cat.category_id] = None
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
        if not self.valid:
            return True
        if not other.valid:
            return False
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
    __slots__ = ['_seconds']

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
        self._seconds = seconds

    @property
    def valid(self):
        """
        Returns `True` if and only if this possession time has a valid
        representation.

        Invalid possession times cannot be compared with other
        possession times.
        """
        return self._seconds is not None

    @property
    def total_seconds(self):
        """
        The total seconds elapsed for this possession.
        `0` is returned if this is not a valid possession time.
        """
        return self._seconds if self.valid else 0

    @property
    def minutes(self):
        """
        The number of whole minutes for a possession.
        e.g., `0:59` would be `0` minutes and `4:01` would be `4`
        minutes.
        `0` is returned if this is not a valid possession time.
        """
        return (self._seconds // 60) if self.valid else 0

    @property
    def seconds(self):
        """
        The seconds portion of the possession time.
        e.g., `0:59` would be `59` seconds and `4:01` would be `1`
        second.
        `0` is returned if this is not a valid possession time.
        """
        return (self._seconds % 60) if self.valid else 0

    def __str__(self):
        if not self.valid:
            return 'N/A'
        else:
            return '%02d:%02d' % (self.minutes, self.seconds)

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        assert self.valid and other.valid
        return self._seconds < other._seconds

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self._seconds == other._seconds

    def __conform__(self, proto):
        if proto is ISQLQuote:
            if not self.valid:
                return AsIs("NULL")
            else:
                return AsIs("ROW(%d)::pos_period" % self._seconds)
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

    def add_seconds(self, seconds):
        """
        Adds the number of seconds given to the current clock time
        and returns a new clock time. `seconds` may be positive
        or negative. If a boundary is reached (e.g., `Pregame` or
        `Final`), then subtracting or adding more seconds has no
        effect.
        """
        elapsed = self.elapsed + seconds
        phase_jump = 0
        if elapsed < 0 or elapsed > Clock._phase_max:
            phase_jump = elapsed // Clock._phase_max

        # Always skip over halftime.
        phase_val = self.phase.value + phase_jump
        if self.phase.value <= Enums.game_phase.Half.value <= phase_val:
            phase_val += 1
        elif phase_val <= Enums.game_phase.Half.value <= self.phase.value:
            phase_val -= 1

        try:
            phase = Enums.game_phase(phase_val)
            return Clock(phase, elapsed % (1 + Clock._phase_max))
        except ValueError:
            if phase_val < 0:
                return Clock(Enums.game_phase.Pregame, 0)
            return Clock(Enums.game_phase.Final, 0)

    @property
    def minutes(self):
        """
        If the clock has a time component, then the number of whole
        minutes **left in this phase** is returned. Otherwise, `0` is
        returned.
        """
        if self.elapsed == 0:
            return 0
        return (Clock._phase_max - self.elapsed) // 60

    @property
    def seconds(self):
        """
        If the clock has a time component, then the number of seconds
        **left in this phase** is returned. Otherwise, `0` is returned.
        """
        if self.elapsed == 0:
            return 0
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


class SQLPlayer (sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['player_id'],
        'managed': ['player'],
        'tables': [
            ('player', ['gsis_name', 'full_name', 'first_name',
                        'last_name', 'team', 'position', 'profile_id',
                        'profile_url', 'uniform_number', 'birthdate',
                        'college', 'height', 'weight', 'years_pro', 'status',
                        ]),
        ],
        'derived': [],
    }


class Player (SQLPlayer):
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
    __slots__ = SQLPlayer.sql_fields() + ['_db']

    _existing = None
    """
    A cache of existing player ids in the database.
    This is only used when saving data to detect if a player
    needs to be added.
    """

    @staticmethod
    def _from_nflgame(db, p):
        """
        Given `p` as a `nflgame.player.PlayPlayerStats` object,
        `_from_nflgame` converts `p` to a `nfldb.Player` object.
        """
        dbp = Player(db)
        dbp.player_id = p.playerid
        dbp.gsis_name = p.name

        if p.player is not None:
            meta = ['full_name', 'first_name', 'last_name', 'team', 'position',
                    'profile_id', 'profile_url', 'uniform_number', 'birthdate',
                    'college', 'height', 'weight', 'years_pro', 'status']
            for k in meta:
                v = getattr(p.player, k, '')
                if not v:
                    # Normalize all empty values to `None`
                    v = None
                setattr(dbp, k, v)

            # Convert position and status values to an enumeration.
            dbp.position = getattr(Enums.player_pos,
                                   dbp.position or '',
                                   Enums.player_pos.UNK)

            trans = Enums._nflgame_player_status
            dbp.status = trans.get(dbp.status or '',
                                   Enums.player_status.Unknown)

        if getattr(dbp, 'position', None) is None:
            dbp.position = Enums.player_pos.UNK
        if getattr(dbp, 'status', None) is None:
            dbp.status = Enums.player_status.Unknown

        dbp.team = nfldb.team.standard_team(getattr(dbp, 'team', ''))
        return dbp

    @staticmethod
    def _from_nflgame_player(db, p):
        """
        Given `p` as a `nflgame.player.Player` object,
        `_from_nflgame_player` converts `p` to a `nfldb.Player` object.
        """
        # This hack translates `nflgame.player.Player` to something like
        # a `nflgame.player.PlayPlayerStats` object that can be converted
        # with `nfldb.Player._from_nflgame`.
        class _Player (object):
            def __init__(self):
                self.playerid = p.player_id
                self.name = p.gsis_name
                self.player = p
        return Player._from_nflgame(db, _Player())

    @staticmethod
    def from_id(db, player_id):
        """
        Given a player GSIS identifier (e.g., `00-0019596`) as a string,
        returns a `nfldb.Player` object corresponding to `player_id`.
        This function will always execute a single SQL query.

        If no corresponding player is found, `None` is returned.
        """
        import nfldb.query
        q = nfldb.query.Query(db)
        players = q.player(player_id=player_id).limit(1).as_players()
        if len(players) == 0:
            return None
        return players[0]

    def __init__(self, db):
        """
        Creates a new and empty `nfldb.Player` object with the given
        database connection.

        This constructor should not be used by clients. Instead, you
        should get `nfldb.Player` objects from `nfldb.Query` or from
        one of the other constructors, like `nfldb.Player.from_id` or
        `nfldb.Player.from_row_dict`. (The latter is useful only if
        you're writing your own SQL queries.)
        """
        self._db = db

        self.player_id = None
        """
        The player_id linking this object `nfldb.PlayPlayer` object.

        N.B. This is the GSIS identifier string. It always has length
        10.
        """
        self.gsis_name = None
        """
        The name of a player from the source GameCenter data. This
        field is guaranteed to contain a name.
        """
        self.full_name = None
        """The full name of a player."""
        self.first_name = None
        """The first name of a player."""
        self.last_name = None
        """The last name of a player."""
        self.team = None
        """
        The team that the player is currently active on. If the player
        is no longer playing or is a free agent, this value may
        correspond to the `UNK` (unknown) team.
        """
        self.position = None
        """
        The current position of a player if it's available. This may
        be **not** be `None`. If the position is not known, then the
        `UNK` enum is used from `nfldb.Enums.player_pos`.
        """
        self.profile_id = None
        """
        The profile identifier used on a player's canonical NFL.com
        profile page. This is used as a foreign key to connect varying
        sources of information.
        """
        self.profile_url = None
        """The NFL.com profile URL for this player."""
        self.uniform_number = None
        """A player's uniform number as an integer."""
        self.birthdate = None
        """A player's birth date as a free-form string."""
        self.college = None
        """A player's college as a free-form string."""
        self.height = None
        """A player's height as a free-form string."""
        self.weight = None
        """A player's weight as a free-form string."""
        self.years_pro = None
        """The number of years a player has played as an integer."""
        self.status = None
        """The current status of this player as a free-form string."""

    def _save(self, cursor):
        if Player._existing is None:
            Player._existing = set()
            cursor.execute('SELECT player_id FROM player')
            for row in cursor.fetchall():
                Player._existing.add(row['player_id'])
        if self.player_id not in Player._existing:
            super(Player, self)._save(cursor)
            Player._existing.add(self.player_id)

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


class SQLPlayPlayer (sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['gsis_id', 'drive_id', 'play_id', 'player_id'],
        'managed': ['play_player'],
        'tables': [('play_player', ['team'] + _player_categories.keys())],
        'derived': ['offense_yds', 'offense_tds', 'defense_tds', 'points'],
    }

    # These fields are combined using `GREATEST`.
    _derived_combined = {
        'offense_yds': ['passing_yds', 'rushing_yds', 'receiving_yds',
                        'fumbles_rec_yds'],
        'offense_tds': ['passing_tds', 'receiving_tds', 'rushing_tds',
                        'fumbles_rec_tds'],
        'defense_tds': ['defense_frec_tds', 'defense_int_tds',
                        'defense_misc_tds'],
    }

    _point_values = [
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

    @classmethod
    def _sql_field(cls, name, aliases=None):
        if name in cls._derived_combined:
            fields = cls._derived_combined[name]
            fields = [cls._sql_field(f, aliases=aliases) for f in fields]
            return 'GREATEST(%s)' % ', '.join(fields)
        elif name == 'points':
            fields = ['(%s * %d)' % (cls._sql_field(f, aliases=aliases), pval)
                      for f, pval in cls._point_values]
            return 'GREATEST(%s)' % ', '.join(fields)
        else:
            return super(SQLPlayPlayer, cls)._sql_field(name, aliases=aliases)


class PlayPlayer (SQLPlayPlayer):
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
    __slots__ = SQLPlayPlayer.sql_fields() \
        + ['_db', '_play', '_player', '_fields']

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
    __pdoc__['PlayPlayer.points'] = \
        """
        The number of points scored in this player statistic. This
        accounts for touchdowns, extra points, two point conversions,
        field goals and safeties.
        """

    @staticmethod
    def _from_nflgame(db, p, pp):
        """
        Given `p` as a `nfldb.Play` object and `pp` as a
        `nflgame.player.PlayPlayerStats` object, `_from_nflgame`
        converts `pp` to a `nfldb.PlayPlayer` object.
        """
        team = nfldb.team.standard_team(pp.team)

        dbpp = PlayPlayer(db)
        dbpp.gsis_id = p.gsis_id
        dbpp.drive_id = p.drive_id
        dbpp.play_id = p.play_id
        dbpp.player_id = pp.playerid
        dbpp.team = team
        for k in _player_categories.keys():
            if pp._stats.get(k, 0) != 0:
                setattr(dbpp, k, pp._stats[k])

        dbpp._play = p
        dbpp._player = Player._from_nflgame(db, pp)
        return dbpp

    @staticmethod
    def fill_plays(db, play_players):
        """
        Given a list of `play_players`, fill all of their `play` attributes
        using as few queries as possible. This will also fill the
        plays with drive data and each drive with game data.
        """
        _fill(db, Play, play_players, '_play')
        Play.fill_drives(db, [pp._play for pp in play_players])
        Drive.fill_games(db, [pp._play._drive for pp in play_players])

    @staticmethod
    def fill_players(db, play_players):
        """
        Given a list of `play_players`, fill all of their `player`
        attributes using as few queries as possible.
        """
        _fill(db, Player, play_players, '_player')

    def __init__(self, db):
        """
        Creates a new and empty `nfldb.PlayPlayer` object with the
        given database connection.

        This constructor should not be used by clients. Instead,
        you should get `nfldb.PlayPlayer` objects
        from `nfldb.Query` or from one of the other
        constructors, like `nfldb.PlayPlayer.from_id` or
        `nfldb.PlayPlayer.from_row_dict`. (The latter is useful only if
        you're writing your own SQL queries.)
        """
        self._db = db
        self._play = None
        self._player = None
        self._fields = None

        self.gsis_id = None
        """
        The GSIS identifier for the game that this "play player"
        belongs to.
        """
        self.drive_id = None
        """
        The numeric drive identifier for this "play player". It may be
        interpreted as a sequence number.
        """
        self.play_id = None
        """
        The numeric play identifier for this "play player". It can
        typically be interpreted as a sequence number scoped to its
        corresponding game.
        """
        self.player_id = None
        """
        The player_id linking these stats to a `nfldb.Player` object.
        Use `nfldb.PlayPlayer.player` to access player meta data.

        N.B. This is the GSIS identifier string. It always has length
        10.
        """
        self.team = None
        """
        The team that this player belonged to when he recorded the
        statistics in this play.
        """

    @property
    def fields(self):
        """The set of non-zero statistical fields set."""
        if self._fields is None:
            self._fields = set()
            for k in _player_categories.keys():
                if getattr(self, k, 0) != 0:
                    self._fields.add(k)
        return self._fields

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
    def scoring_team(self):
        """
        If this is a scoring statistic, returns the team that scored.
        Otherwise, returns None.

        N.B. `nfldb.PlayPlayer.scoring_team` returns a valid team if
        and only if `nfldb.PlayPlayer.points` is greater than 0.
        """
        if self.points > 0:
            return self.team
        return None

    @property
    def guess_position(self):
        """
        Guesses the position of this player based on the statistical
        categories present.

        Note that this only distinguishes the offensive positions of
        QB, RB, WR, P and K. If defensive stats are detected, then
        the position returned defaults to LB.
        """
        stat_to_pos = [
            ('passing_att', 'QB'), ('rushing_att', 'RB'),
            ('receiving_tar', 'WR'), ('punting_tot', 'P'),
            ('kicking_tot', 'K'), ('kicking_fga', 'K'), ('kicking_xpa', 'K'),
        ]
        for c in stat_categories:
            if c.startswith('defense_'):
                stat_to_pos.append((c, 'LB'))
        for stat, pos in stat_to_pos:
            if getattr(self, stat) != 0:
                return Enums.player_pos[pos]
        return Enums.player_pos.UNK

    def _save(self, cursor):
        if self._player is not None:
            self._player._save(cursor)
        super(PlayPlayer, self)._save(cursor)

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
            setattr(a, cat, getattr(a, cat) + getattr(b, cat))

        # Try to copy player meta data too.
        if a._player is None and b._player is not None:
            a._player = b._player

        # A play attached to aggregate statistics is always wrong.
        a._play = None

    def _copy(self):
        """Returns a copy of `self`."""
        pp = PlayPlayer(self._db)
        pp.gsis_id = self.gsis_id
        pp.drive_id = self.drive_id
        pp.play_id = self.play_id
        pp.player_id = self.player_id
        pp.team = self.team

        ga, sa = getattr, setattr
        for k in _player_categories:
            v = getattr(self, k, 0)
            if v != 0:
                sa(pp, k, v)
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


class SQLPlay (sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['gsis_id', 'drive_id', 'play_id'],
        'managed': ['play'],
        'tables': [
            ('play', ['time', 'pos_team', 'yardline', 'down', 'yards_to_go',
                      'description', 'note', 'time_inserted', 'time_updated',
                      ] + _play_categories.keys()),
            ('agg_play', _player_categories.keys()),
        ],
        'derived': ['offense_yds', 'offense_tds', 'defense_tds', 'points',
                    'game_date'],
    }

    @classmethod
    def _sql_field(cls, name, aliases=None):
        if name in PlayPlayer._derived_combined:
            fields = [cls._sql_field(f, aliases=aliases)
                      for f in PlayPlayer._derived_combined[name]]
            return 'GREATEST(%s)' % ', '.join(fields)
        elif name == 'points':
            fields = ['(%s * %d)' % (cls._sql_field(f, aliases=aliases), pval)
                      for f, pval in PlayPlayer._point_values]
            return 'GREATEST(%s)' % ', '.join(fields)
        elif name == 'game_date':
            gsis_id = cls._sql_field('gsis_id', aliases=aliases)
            return 'SUBSTRING(%s from 1 for 8)' % gsis_id
        else:
            return super(SQLPlay, cls)._sql_field(name, aliases=aliases)


class Play (SQLPlay):
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
    __slots__ = SQLPlay.sql_fields() + ['_db', '_drive', '_play_players']

    # Document instance variables for derived SQL fields.
    # We hide them from the public interface, but make the doco
    # available to nfldb-mk-stat-table. Evil!
    __pdoc__['Play.offense_yds'] = None
    __pdoc__['_Play.offense_yds'] = \
        '''
        Corresponds to any yardage that is manufactured by the offense.
        Namely, the following fields:
        `nfldb.Play.passing_yds`,
        `nfldb.Play.rushing_yds`,
        `nfldb.Play.receiving_yds` and
        `nfldb.Play.fumbles_rec_yds`.

        This field is useful when searching for plays by net yardage
        regardless of how the yards were obtained.
        '''
    __pdoc__['Play.offense_tds'] = None
    __pdoc__['_Play.offense_tds'] = \
        '''
        Corresponds to any touchdown manufactured by the offense via
        a passing, reception, rush or fumble recovery.
        '''
    __pdoc__['Play.defense_tds'] = None
    __pdoc__['_Play.defense_tds'] = \
        '''
        Corresponds to any touchdown manufactured by the defense.
        e.g., a pick-6, fumble recovery TD, punt/FG block TD, etc.
        '''
    __pdoc__['Play.points'] = \
        """
        The number of points scored in this player statistic. This
        accounts for touchdowns, extra points, two point conversions,
        field goals and safeties.
        """

    @staticmethod
    def _from_nflgame(db, d, p):
        """
        Given `d` as a `nfldb.Drive` object and `p` as a
        `nflgame.game.Play` object, `_from_nflgame` converts `p` to a
        `nfldb.Play` object.
        """
        # Fix up some fields so they meet the constraints of the schema.
        # The `time` field is cleaned up afterwards in
        # `nfldb.Drive._from_nflgame`, since it needs data about surrounding
        # plays.
        time = None if not p.time else _nflgame_clock(p.time)
        yardline = FieldPosition(getattr(p.yardline, 'offset', None))
        down = p.down if 1 <= p.down <= 4 else None
        team = p.team if p.team is not None and len(p.team) > 0 else 'UNK'

        dbplay = Play(db)
        dbplay.gsis_id = d.gsis_id
        dbplay.drive_id = d.drive_id
        dbplay.play_id = int(p.playid)
        dbplay.time = time
        dbplay.pos_team = team
        dbplay.yardline = yardline
        dbplay.down = down
        dbplay.yards_to_go = p.yards_togo
        dbplay.description = p.desc
        dbplay.note = p.note
        for k in _play_categories.keys():
            if p._stats.get(k, 0) != 0:
                setattr(dbplay, k, p._stats[k])
        # Note that `Play` objects also normally contain aggregated
        # statistics, but we forgo that here because this constructor
        # is only used to load plays into the database.

        dbplay._drive = d
        dbplay._play_players = []
        for pp in p.players:
            dbpp = PlayPlayer._from_nflgame(db, dbplay, pp)
            dbplay._play_players.append(dbpp)
        return dbplay

    @staticmethod
    def from_id(db, gsis_id, drive_id, play_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string,
        an integer drive id and an integer play id, this returns a
        `nfldb.Play` object corresponding to the given identifiers.

        If no corresponding play is found, then `None` is returned.
        """
        import nfldb.query
        q = nfldb.query.Query(db)
        q.play(gsis_id=gsis_id, drive_id=drive_id, play_id=play_id).limit(1)
        plays = q.as_plays()
        if len(plays) == 0:
            return None
        return plays[0]

    @staticmethod
    def fill_drives(db, plays):
        """
        Given a list of `plays`, fill all of their `drive` attributes
        using as few queries as possible. This will also fill the
        drives with game data.
        """
        _fill(db, Drive, plays, '_drive')
        Drive.fill_games(db, [p._drive for p in plays])

    def __init__(self, db):
        """
        Creates a new and empty `nfldb.Play` object with the given
        database connection.

        This constructor should not be used by clients. Instead, you
        should get `nfldb.Play` objects from `nfldb.Query` or from one
        of the other constructors, like `nfldb.Play.from_id` or
        `nfldb.Play.from_row_dict`. (The latter is useful only if you're
        writing your own SQL queries.)
        """
        self._db = db
        self._drive = None
        self._play_players = None

        self.gsis_id = None
        """
        The GSIS identifier for the game that this play belongs to.
        """
        self.drive_id = None
        """
        The numeric drive identifier for this play. It may be
        interpreted as a sequence number.
        """
        self.play_id = None
        """
        The numeric play identifier for this play. It can typically
        be interpreted as a sequence number scoped to the week that
        this game was played, but it's unfortunately not completely
        consistent.
        """
        self.time = None
        """
        The time on the clock when the play started, represented with
        a `nfldb.Clock` object.
        """
        self.pos_team = None
        """
        The team in possession during this play, represented as
        a team abbreviation string. Use the `nfldb.Team` constructor
        to get more information on a team.
        """
        self.yardline = None
        """
        The starting field position of this play represented with
        `nfldb.FieldPosition`.
        """
        self.down = None
        """
        The down on which this play begin. This may be `0` for
        "special" plays like timeouts or 2 point conversions.
        """
        self.yards_to_go = None
        """
        The number of yards to go to get a first down or score a
        touchdown at the start of the play.
        """
        self.description = None
        """
        A (basically) free-form text description of the play. This is
        typically what you see on NFL GameCenter web pages.
        """
        self.note = None
        """
        A miscellaneous note field (as a string). Not sure what it's
        used for.
        """
        self.time_inserted = None
        """
        The date and time that this play was added to the
        database. This can be very useful when sorting plays by the
        order in which they occurred in real time. Unfortunately, such
        a sort requires that play data is updated relatively close to
        when it actually occurred.
        """
        self.time_updated = None
        """The date and time that this play was last updated."""

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
            import nfldb.query
            q = nfldb.query.Query(self._db)
            q.play_player(gsis_id=self.gsis_id, drive_id=self.drive_id,
                          play_id=self.play_id)
            self._play_players = q.as_play_players()
            for pp in self._play_players:
                pp._play = self
        return self._play_players

    @property
    def scoring_team(self):
        """
        If this is a scoring play, returns the team that scored points.
        Otherwise, returns None.

        N.B. `nfldb.Play.scoring_team` returns a valid team if and only
        if `nfldb.Play.points` is greater than 0.
        """
        for pp in self.play_players:
            t = pp.scoring_team
            if t is not None:
                return t
        return None

    def score(self, before=False):
        """
        Returns the score of the game immediately after this play as a
        tuple of the form `(home_score, away_score)`.

        If `before` is `True`, then the score will *not* include this
        play.
        """
        game = Game.from_id(self._db, self.gsis_id)
        if not before:
            return game.score_at_time(self.time.add_seconds(1))

        s = game.score_at_time(self.time)
        # The heuristic in `nfldb.Game.score_in_plays` blends TDs and XPs
        # into a single play (with respect to scoring). So we have to undo
        # that if we want the score of the game after a TD but before an XP.
        if self.kicking_xpmade == 1:
            score_team = self.scoring_team
            if score_team == game.home_team:
                return (s[0] - 1, s[1])
            return (s[0], s[1] - 1)
        return s

    def _save(self, cursor):
        super(Play, self)._save(cursor)

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
        raise AttributeError(k)


class SQLDrive (sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['gsis_id', 'drive_id'],
        'managed': ['drive'],
        'tables': [
            ('drive', ['start_field', 'start_time', 'end_field', 'end_time',
                       'pos_team', 'pos_time', 'first_downs', 'result',
                       'penalty_yards', 'yards_gained', 'play_count',
                       'time_inserted', 'time_updated',
                       ]),
        ],
        'derived': [],
    }


class Drive (SQLDrive):
    """
    Represents a single drive in an NFL game. Each drive has an
    assortment of meta data, possibly including the start and end
    times, the start and end field positions, the result of the drive,
    the number of penalties and first downs, and more.

    Each drive corresponds to **zero or more** plays. A drive usually
    corresponds to at least one play, but if the game is active, there
    exist valid ephemeral states where a drive has no plays.
    """
    __slots__ = SQLDrive.sql_fields() + ['_db', '_game', '_plays']

    @staticmethod
    def _from_nflgame(db, g, d):
        """
        Given `g` as a `nfldb.Game` object and `d` as a
        `nflgame.game.Drive` object, `_from_nflgame` converts `d` to a
        `nfldb.Drive` object.

        Generally, this function should not be used. It is called
        automatically by `nfldb.Game._from_nflgame`.
        """
        dbd = Drive(db)
        dbd.gsis_id = g.gsis_id
        dbd.drive_id = d.drive_num
        dbd.start_time = _nflgame_clock(d.time_start)
        dbd.start_field = FieldPosition(getattr(d.field_start, 'offset', None))
        dbd.end_field = FieldPosition(d.field_end.offset)
        dbd.end_time = _nflgame_clock(d.time_end)
        dbd.pos_team = nfldb.team.standard_team(d.team)
        dbd.pos_time = PossessionTime(d.pos_time.total_seconds())
        dbd.first_downs = d.first_downs
        dbd.result = d.result
        dbd.penalty_yards = d.penalty_yds
        dbd.yards_gained = d.total_yds
        dbd.play_count = d.play_cnt

        dbd._game = g
        candidates = []
        for play in d.plays:
            candidates.append(Play._from_nflgame(db, dbd, play))

        # At this point, some plays don't have valid game times. Fix it!
        # If we absolutely cannot fix it, drop the play. Maintain integrity!
        dbd._plays = []
        for play in candidates:
            if play.time is None:
                next = _next_play_with(candidates, play, lambda p: p.time)
                play.time = _play_time(dbd, play, next)
            if play.time is not None:
                dbd._plays.append(play)
        dbd._plays.sort(key=lambda p: p.play_id)
        return dbd

    @staticmethod
    def from_id(db, gsis_id, drive_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string
        and a integer drive id, this returns a `nfldb.Drive` object
        corresponding to the given identifiers.

        If no corresponding drive is found, then `None` is returned.
        """
        import nfldb.query
        q = nfldb.query.Query(db)
        q.drive(gsis_id=gsis_id, drive_id=drive_id).limit(1)
        drives = q.as_drives()
        if len(drives) == 0:
            return None
        return drives[0]

    @staticmethod
    def fill_games(db, drives):
        """
        Given a list of `drives`, fill all of their `game` attributes
        using as few queries as possible.
        """
        _fill(db, Game, drives, '_game')

    def __init__(self, db):
        """
        Creates a new and empty `nfldb.Drive` object with the given
        database connection.

        This constructor should not be used by clients. Instead, you
        should get `nfldb.Drive` objects from `nfldb.Query` or from one
        of the other constructors, like `nfldb.Drive.from_id` or
        `nfldb.Drive.from_row_dict`. (The latter is useful only if you're
        writing your own SQL queries.)
        """
        self._db = db
        self._game = None
        self._plays = None

        self.gsis_id = None
        """
        The GSIS identifier for the game that this drive belongs to.
        """
        self.drive_id = None
        """
        The numeric drive identifier for this drive. It may be
        interpreted as a sequence number.
        """
        self.start_field = None
        """
        The starting field position of this drive represented
        with `nfldb.FieldPosition`.
        """
        self.start_time = None
        """
        The starting clock time of this drive, represented with
        `nfldb.Clock`.
        """
        self.end_field = None
        """
        The ending field position of this drive represented with
        `nfldb.FieldPosition`.
        """
        self.end_time = None
        """
        The ending clock time of this drive, represented with
        `nfldb.Clock`.
        """
        self.pos_team = None
        """
        The team in possession during this drive, represented as
        a team abbreviation string. Use the `nfldb.Team` constructor
        to get more information on a team.
        """
        self.pos_time = None
        """
        The possession time of this drive, represented with
        `nfldb.PossessionTime`.
        """
        self.first_downs = None
        """
        The number of first downs that occurred in this drive.
        """
        self.result = None
        """
        A freeform text field straight from NFL's GameCenter data that
        sometimes contains the result of a drive (e.g., `Touchdown`).
        """
        self.penalty_yards = None
        """
        The number of yards lost or gained from penalties in this
        drive.
        """
        self.yards_gained = None
        """
        The total number of yards gained or lost in this drive.
        """
        self.play_count = None
        """
        The total number of plays executed by the offense in this
        drive.
        """
        self.time_inserted = None
        """The date and time that this drive was added."""
        self.time_updated = None
        """The date and time that this drive was last updated."""

    @property
    def game(self):
        """
        Returns the `nfldb.Game` object that contains this drive. The
        game is retrieved from the database if it hasn't been already.
        """
        if self._game is None:
            return Game.from_id(self._db, self.gsis_id)
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
            import nfldb.query
            q = nfldb.query.Query(self._db)
            q.sort([('time', 'asc'), ('play_id', 'asc')])
            q.play(gsis_id=self.gsis_id, drive_id=self.drive_id)
            self._plays = q.as_plays()
            for p in self._plays:
                p._drive = self
        return self._plays

    def score(self, before=False):
        """
        Returns the score of the game immediately after this drive as a
        tuple of the form `(home_score, away_score)`.

        If `before` is `True`, then the score will *not* include this
        drive.
        """
        if before:
            return self.game.score_at_time(self.start_time)
        else:
            return self.game.score_at_time(self.end_time)

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

    def _save(self, cursor):
        super(Drive, self)._save(cursor)
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


class SQLGame (sql.Entity):
    __slots__ = []

    _sql_tables = {
        'primary': ['gsis_id'],
        'managed': ['game'],
        'tables': [
            ('game', ['gamekey', 'start_time', 'week', 'day_of_week',
                      'season_year', 'season_type', 'finished',
                      'home_team', 'home_score', 'home_score_q1',
                      'home_score_q2', 'home_score_q3', 'home_score_q4',
                      'home_score_q5', 'home_turnovers',
                      'away_team', 'away_score', 'away_score_q1',
                      'away_score_q2', 'away_score_q3', 'away_score_q4',
                      'away_score_q5', 'away_turnovers',
                      'time_inserted', 'time_updated']),
        ],
        'derived': ['winner', 'loser'],
    }

    @classmethod
    def _sql_field(cls, name, aliases=None):
        if name in ('winner', 'loser'):
            params = ('home_score', 'away_score', 'home_team', 'away_team')
            d = dict([(k, cls._sql_field(k, aliases=aliases)) for k in params])
            d['cmp'] = '>' if name == 'winner' else '<'
            return '''(
                CASE WHEN {home_score} {cmp} {away_score} THEN {home_team}
                     WHEN {away_score} {cmp} {home_score} THEN {away_team}
                     ELSE ''
                END
            )'''.format(**d)
        else:
            return super(SQLGame, cls)._sql_field(name, aliases=aliases)


class Game (SQLGame):
    """
    Represents a single NFL game in the preseason, regular season or
    post season. Each game has an assortment of meta data, including
    a quarterly breakdown of scores, turnovers, the time the game
    started, the season week the game occurred in, and more.

    Each game corresponds to **zero or more** drives. A game usually
    corresponds to at least one drive, but if the game is active, there
    exist valid ephemeral states where a game has no drives.
    """
    __slots__ = SQLGame.sql_fields() + ['_db', '_drives', '_plays']

    # Document instance variables for derived SQL fields.
    __pdoc__['Game.winner'] = '''The winner of this game.'''
    __pdoc__['Game.loser'] = '''The loser of this game.'''

    @staticmethod
    def _from_nflgame(db, g):
        """
        Converts a `nflgame.game.Game` object to a `nfldb.Game`
        object.

        `db` should be a psycopg2 connection returned by
        `nfldb.connect`.
        """
        dbg = Game(db)
        dbg.gsis_id = g.eid
        dbg.gamekey = g.gamekey
        dbg.start_time = _nflgame_start_time(g.schedule)
        dbg.week = g.schedule['week']
        dbg.day_of_week = Enums._nflgame_game_day[g.schedule['wday']]
        dbg.season_year = g.schedule['year']
        dbg.season_type = Enums._nflgame_season_phase[g.schedule['season_type']]
        dbg.finished = g.game_over()
        dbg.home_team = nfldb.team.standard_team(g.home)
        dbg.home_score = g.score_home
        dbg.home_score_q1 = g.score_home_q1
        dbg.home_score_q2 = g.score_home_q2
        dbg.home_score_q3 = g.score_home_q3
        dbg.home_score_q4 = g.score_home_q4
        dbg.home_score_q5 = g.score_home_q5
        dbg.home_turnovers = int(g.data['home']['to'])
        dbg.away_team = nfldb.team.standard_team(g.away)
        dbg.away_score = g.score_away
        dbg.away_score_q1 = g.score_away_q1
        dbg.away_score_q2 = g.score_away_q2
        dbg.away_score_q3 = g.score_away_q3
        dbg.away_score_q4 = g.score_away_q4
        dbg.away_score_q5 = g.score_away_q5
        dbg.away_turnovers = int(g.data['away']['to'])

        # If it's been 8 hours since game start, we always conclude finished!
        if (now() - dbg.start_time).total_seconds() >= (60 * 60 * 8):
            dbg.finished = True

        dbg._drives = []
        for drive in g.drives:
            if not hasattr(drive, 'game'):
                continue
            dbg._drives.append(Drive._from_nflgame(db, dbg, drive))
        dbg._drives.sort(key=lambda d: d.drive_id)
        return dbg

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
    def from_id(db, gsis_id):
        """
        Given a GSIS identifier (e.g., `2012090500`) as a string,
        returns a `nfldb.Game` object corresponding to `gsis_id`.

        If no corresponding game is found, `None` is returned.
        """
        import nfldb.query
        q = nfldb.query.Query(db)
        games = q.game(gsis_id=gsis_id).limit(1).as_games()
        if len(games) == 0:
            return None
        return games[0]

    def __init__(self, db):
        """
        Creates a new and empty `nfldb.Game` object with the given
        database connection.

        This constructor should not be used by clients. Instead, you
        should get `nfldb.Game` objects from `nfldb.Query` or from one
        of the other constructors, like `nfldb.Game.from_id` or
        `nfldb.Game.from_row_dict`. (The latter is useful only if you're
        writing your own SQL queries.)
        """
        self._db = db
        """
        The psycopg2 database connection.
        """
        self._drives = None
        self._plays = None

        self.gsis_id = None
        """
        The NFL GameCenter id of the game. It is a string
        with 10 characters. The first 8 correspond to the date of the
        game, while the last 2 correspond to an id unique to the week that
        the game was played.
        """
        self.gamekey = None
        """
        Another unique identifier for a game used by the
        NFL. It is a sequence number represented as a 5 character string.
        The gamekey is specifically used to tie games to other resources,
        like the NFL's content delivery network.
        """
        self.start_time = None
        """
        A Python datetime object corresponding to the start time of
        the game. The timezone of this value will be equivalent to the
        timezone specified by `nfldb.set_timezone` (which is by default
        set to the value specified in the configuration file).
        """
        self.week = None
        """
        The week number of this game. It is always relative
        to the phase of the season. Namely, the first week of preseason
        is 1 and so is the first week of the regular season.
        """
        self.day_of_week = None
        """
        The day of the week this game was played on.
        Possible values correspond to the `nfldb.Enums.game_day` enum.
        """
        self.season_year = None
        """
        The year of the season of this game. This
        does not necessarily match the year that the game was played. For
        example, games played in January 2013 are in season 2012.
        """
        self.season_type = None
        """
        The phase of the season. e.g., `Preseason`,
        `Regular season` or `Postseason`. All valid values correspond
        to the `nfldb.Enums.season_phase`.
        """
        self.finished = None
        """
        A boolean that is `True` if and only if the game has finished.
        """
        self.home_team = None
        """
        The team abbreviation for the home team. Use the `nfldb.Team`
        constructor to get more information on a team.
        """
        self.home_score = None
        """The current total score for the home team."""
        self.home_score_q1 = None
        """The 1st quarter score for the home team."""
        self.home_score_q2 = None
        """The 2nd quarter score for the home team."""
        self.home_score_q3 = None
        """The 3rd quarter score for the home team."""
        self.home_score_q4 = None
        """The 4th quarter score for the home team."""
        self.home_score_q5 = None
        """The OT quarter score for the home team."""
        self.home_turnovers = None
        """Total turnovers for the home team."""
        self.away_team = None
        """
        The team abbreviation for the away team. Use the `nfldb.Team`
        constructor to get more information on a team.
        """
        self.away_score = None
        """The current total score for the away team."""
        self.away_score_q1 = None
        """The 1st quarter score for the away team."""
        self.away_score_q2 = None
        """The 2nd quarter score for the away team."""
        self.away_score_q3 = None
        """The 3rd quarter score for the away team."""
        self.away_score_q4 = None
        """The 4th quarter score for the away team."""
        self.away_score_q5 = None
        """The OT quarter score for the away team."""
        self.away_turnovers = None
        """Total turnovers for the away team."""
        self.time_inserted = None
        """The date and time that this game was added."""
        self.time_updated = None
        """The date and time that this game was last updated."""
        self.winner = None
        """The team abbreviation for the winner of this game."""
        self.loser = None
        """The team abbreviation for the loser of this game."""

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
            import nfldb.query
            q = nfldb.query.Query(self._db)
            self._drives = q.drive(gsis_id=self.gsis_id).as_drives()
            for d in self._drives:
                d._game = self
        return self._drives

    @property
    def plays(self):
        """
        A list of `nfldb.Play` objects in this game. Data is retrieved
        from the database if it hasn't been already.
        """
        if self._plays is None:
            import nfldb.query
            q = nfldb.query.Query(self._db)
            q.sort([('time', 'asc'), ('play_id', 'asc')])
            self._plays = q.play(gsis_id=self.gsis_id).as_plays()
        return self._plays

    def plays_range(self, start, end):
        """
        Returns a list of `nfldb.Play` objects for this game in the
        time range specified. The range corresponds to a half-open
        interval, i.e., `[start, end)`. Namely, all plays starting at
        or after `start` up to plays starting *before* `end`.

        The plays are returned in the order in which they occurred.

        `start` and `end` should be instances of the
        `nfldb.Clock` class. (Hint: Values can be created with the
        `nfldb.Clock.from_str` function.)
        """
        import nfldb.query as query

        q = query.Query(self._db)
        q.play(gsis_id=self.gsis_id, time__ge=start, time__lt=end)
        q.sort([('time', 'asc'), ('play_id', 'asc')])
        return q.as_plays()

    def score_in_plays(self, plays):
        """
        Returns the scores made by the home and away teams from the
        sequence of plays given. The scores are returned as a `(home,
        away)` tuple. Note that this method assumes that `plays` is
        sorted in the order in which the plays occurred.
        """
        # This method is a heuristic to compute the total number of points
        # scored in a set of plays. Naively, this should be a simple summation
        # of the `points` attribute of each field. However, it seems that
        # the JSON feed (where this data comes from) heavily biases toward
        # omitting XPs. Therefore, we attempt to add them. A brief outline
        # of the heuristic follows.
        #
        # In *most* cases, a TD is followed by either an XP attempt or a 2 PTC
        # attempt by the same team. Therefore, after each TD, we look for the
        # next play that fits this criteria, while being careful not to find
        # a play that has already counted toward the score. If no play was
        # found, then we assume there was an XP attempt and that it was good.
        # Otherwise, if a play is found matching the given TD, the point total
        # of that play is added to the score.
        #
        # Note that this relies on the property that every TD is paired with
        # an XP/2PTC with respect to the final score of a game. Namely, when
        # searching for the XP/2PTC after a TD, it may find a play that came
        # after a different TD. But this is OK, so long as we never double
        # count any particular play.
        def is_twopta(p):
            return (p.passing_twopta > 0
                    or p.receiving_twopta > 0
                    or p.rushing_twopta > 0)

        counted = set()  # don't double count
        home, away = 0, 0
        for i, p in enumerate(plays):
            pts = p.points
            if pts > 0 and p.play_id not in counted:
                counted.add(p.play_id)

                if pts == 6:
                    def after_td(p2):
                        return (p.pos_team == p2.pos_team
                                and (p2.kicking_xpa > 0 or is_twopta(p2))
                                and p2.play_id not in counted)

                    next = _next_play_with(plays, p, after_td)
                    if next is None:
                        pts += 1
                    elif next.play_id not in counted:
                        pts += next.points
                        counted.add(next.play_id)
                if p.scoring_team == self.home_team:
                    home += pts
                else:
                    away += pts
        return home, away

    def score_at_time(self, time):
        """
        Returns the score of the game at the time specified as a
        `(home, away)` tuple.

        `time` should be an instance of the `nfldb.Clock` class.
        (Hint: Values can be created with the `nfldb.Clock.from_str`
        function.)
        """
        start = Clock.from_str('Pregame', '0:00')
        return self.score_in_plays(self.plays_range(start, time))

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

    def _save(self, cursor):
        super(Game, self)._save(cursor)
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
