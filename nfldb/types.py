from __future__ import absolute_import, division, print_function

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
import datetime

import enum

import psycopg2
from psycopg2.extensions import AsIs, ISQLQuote

import pytz

import nflgame

from nfldb.db import Tx

__pdoc__ = {}


def stat_categories(db):
    """
    Given a database object, returns a `collections.OrderedDict` of all
    statistical categories available for play-by-play data.
    """
    cats = OrderedDict()
    with Tx(db) as cursor:
        cursor.execute('''
            SELECT
                category_id, gsis_number, category_type, is_real, description
            FROM category
            ORDER BY category_type ASC, category_id ASC
        ''')
        for row in cursor.fetchall():
            cats[row['category_id']] = Category.from_row(row)
    return cats


def _nflgame_start_time(g):
    """
    Given a `nflgame.game.Game` object, return the start time of the
    game in UTC.
    """
    # BUG: Getting the hour here will be wrong if a game starts before Noon
    # EST. Not sure what to do about it...
    hour, minute = g.schedule['time'].strip().split(':')
    hour, minute = (int(hour) + 12) % 24, int(minute)
    d = datetime.datetime(g.schedule['year'], g.schedule['month'],
                          g.schedule['day'], hour, minute)
    return pytz.timezone('US/Eastern').localize(d).astimezone(pytz.utc)


def _nflgame_clock(clock):
    """
    Given a `nflgame.game.GameClock` object, convert and return it as
    a `nfldb.Clock` object.
    """
    phase = Enums._nflgame_game_phase[clock.quarter]
    elapsed = Clock._phase_max - ((clock._minutes * 60) + clock._seconds)
    return Clock(phase, elapsed)


class _Enum (enum.Enum):
    """
    Conforms to the `getquoted` interface in psycopg2.
    This maps enum types to SQL.
    """
    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("'%s'" % self.name)
        return None

    @staticmethod
    def _pg_cast(enum):
        """
        Returns a function to cast a SQL enum to the enumeration type
        corresponding to `enum`. Namely, `enum` should be a member of
        `nfldb.Enums`.
        """
        return lambda sqlv, _: enum[sqlv]


class Enums (object):
    """
    Enums groups all enum types used in the database schema.
    All possible values for each enum type are represented as lists.
    The ordering of each list is the same as the ordering in the
    database.
    """

    game_phase = _Enum('game_phase',
                       ['Pregame', 'Q1', 'Q2', 'Half',
                        'Q3', 'Q4', 'OT', 'Final'])
    """
    Represents the phase of the game. e.g., `Q1` or `HALF`.
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

    playerpos = _Enum('playerpos',
                      ['C', 'CB', 'DB', 'DE', 'DL', 'DT', 'FB', 'FS', 'G',
                       'ILB', 'K', 'LB', 'LS', 'MLB', 'NT', 'OG', 'OL', 'OLB',
                       'OT', 'P', 'QB', 'RB', 'SAF', 'SS', 'T', 'TE', 'WR'])
    """
    The set of all possible player positions in abbreviated form.
    """

    category_scope = _Enum('category_scope', ['play', 'player'])
    """
    The scope of a particular statistic. Typically, statistics refer
    to a specific `player`, but sometimes a statistic refers to the
    totality of a play. For example, `third_down_att` is a `play`
    statistic that records third down attempts.

    Currently, `play` and `player` are the only possible values.
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


class Team (object):
    """
    Represents information about an NFL team. This includes its
    standard three letter abbreviation, city and mascot name.
    """
    def __init__(self, abbr):
        """
        Creates a new team its standard abbreviation. The rest of a
        team's information is either captured from cache or from the
        database.
        """
        self.team_id = abbr
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

    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("'%s'" % self.team_id)
        return None


class Category (object):
    """
    Represents meta data about a statistical category. This includes
    the categorie's scope, GSIS identifier, name and short description.
    """
    __slots__ = ['category_id', 'gsis_number', 'category_type',
                 'is_real', 'description']

    @staticmethod
    def from_row(row):
        """
        Introduces a `nfldb.Category` object from a row in the
        `category` table.
        """
        return Category(row['category_id'], row['gsis_number'],
                        row['category_type'], row['is_real'],
                        row['description'])

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
        The SQL definition of this column.
        """
        typ = 'real' if self.is_real else 'smallint'
        return '%s %s NULL' % (self.category_id, typ)

    def __str__(self):
        return self.category_id

    def __eq__(self, other):
        return self.category_id == other.category_id


class FieldPosition (object):
    """
    Represents field position.

    The representation here is an integer offset where the 50 yard line
    corresponds to '0'. Being in the own territory corresponds to a negative
    offset while being in the opponent's territory corresponds to a positive
    offset.

    e.g., NE has the ball on the NE 45, the offset is -5.
    e.g., NE has the ball on the NYG 2, the offset is 48.
    """
    __slots__ = ['__offset']

    def __init__(self, offset):
        """
        Makes a new `nfldb.FieldPosition` given a field `offset`.
        `offset` must be in the integer range [-50, 50].
        """
        if offset is None:
            self.__offset = None
            return
        assert -50 <= offset <= 50
        self.__offset = offset

    def add_yards(self, yards):
        """
        Returns a new `nfldb.FieldPosition` with `yards` added to this
        field position. The value of `yards` may be negative.
        """
        assert self.valid
        newoffset = max(-50, min(50, self.__offset + yards))
        return FieldPosition(newoffset)

    @property
    def valid(self):
        """
        Returns `True` if and only if this field position is known and
        valid.

        Invalid field positions cannot be compared with other field
        positions.
        """
        return self.__offset is not None

    def __lt__(self, other):
        assert self.valid and other.valid
        return self.__offset < other.__offset

    def __eq__(self, other):
        return self.__offset == other.__offset

    def __str__(self):
        if not self.valid:
            return 'N/A'
        elif self.offset > 0:
            return 'OPP %d' % (50 - self.offset)
        elif self.offset < 0:
            return 'OWN %d' % (50 + self.offset)
        else:
            return 'MIDFIELD'

    def __conform__(self, proto):
        if proto is ISQLQuote:
            if not self.valid:
                return AsIs("NULL")
            else:
                return AsIs("%d" % self.__offset)
        return None


class PossessionTime (object):
    """
    Represents the possession time of a drive in seconds.
    """
    __slots__ = ['__seconds']

    @staticmethod
    def clock(clock_str):
        """
        Introduces a `nfldb.PossessionTime` object from a string
        formatted as clock time. For example, `2:00` corresponds to
        `120` seconds and `14:39` corresponds to `879` seconds.
        """
        minutes, seconds = map(int, clock_str.split(':', 1))
        return PossessionTime((minutes * 60) + seconds)

    def __init__(self, seconds):
        """
        Returns a `nfldb.PossessionTime` object given the number of
        seconds of the possession.
        """
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
        Returns the total seconds elapsed for this possession.
        """
        return self.__seconds if self.valid else 0

    @property
    def minutes(self):
        """
        Returns the number of whole minutes for a possession.
        e.g., `0:59` would be `0` minutes and `4:01` would be `4`
        minutes.
        """
        return (self.__seconds // 60) if self.valid else 0

    @property
    def seconds(self):
        """
        Returns the seconds portion of the possession time.
        e.g., `0:59` would be `59` seconds and `4:01` would be `1`
        second.
        """
        return (self.__seconds % 60) if self.valid else 0

    def __str__(self):
        if not self.valid:
            return 'N/A'
        else:
            return '%d:%d' % (self.minutes, self.seconds)

    def __lt__(self, other):
        assert self.valid and other.valid
        return self.__seconds < other.__seconds

    def __eq__(self, other):
        return self.__seconds == other.__seconds

    def __conform__(self, proto):
        if proto is ISQLQuote:
            if not self.valid:
                return AsIs("NULL")
            else:
                return AsIs("%d" % self.__seconds)
        return None


class Clock (object):
    """
    Represents a single point in time during a game. This includes the
    quarter and the game clock time in addition to other phases of the
    game such as before the game starts, half time, overtime and when
    the game ends.

    Note that the clock time does not uniquely identify a play, since
    not all plays consume time on the clock. (e.g., A two point
    conversion.)
    """

    __nonqs = (Enums.game_phase.Pregame, Enums.game_phase.Half,
               Enums.game_phase.Final)
    """
    The phases of the game that do not have a time component.
    """

    _phase_max = 900
    """
    The maximum number of seconds in a game phase.
    """

    def __init__(self, game_phase, elapsed):
        """
        Introduces a new `nfldb.Clock` object. `game_phase` should
        be a value from the `nfldb.Enums.game_phase` enumeration
        while `elapsed` should be the number of seconds elapsed in
        the `game_phase`. Note that `elapsed` is only applicable when
        `game_phase` is a quarter (including overtime). In all other
        cases, it will be set to `0`.

        `elapsed` should be in the range `[0, 900]` where `900`
        corresponds to the clock time `0:00` and `0` corresponds
        to the clock time `15:00`.
        """
        assert isinstance(game_phase, Enums.game_phase)
        assert 0 <= elapsed <= Clock._phase_max

        if game_phase in Clock.__nonqs:
            elapsed = 0

        self.game_phase = game_phase
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
        phase = self.game_phase
        if phase in Clock.__nonqs:
            return phase.name
        else:
            return '%s %d:%d' % (phase.name, self.minutes, self.seconds)

    def __lt__(self, o):
        return (self.game_phase, self.elapsed) < (o.game_phase, o.elapsed)

    def __eq__(self, o):
        return self.game_phase == o.game_phase and self.elapsed == o.elapsed

    def __conform__(self, proto):
        if proto is ISQLQuote:
            return AsIs("ROW('%s', %d)::game_time"
                        % (self.game_phase.name, self.elapsed))
        return None


class Drive (object):
    @staticmethod
    def from_nflgame(g, drive):
        """
        Given `g` as a `nfldb.Game` object and `drive` as a
        `nflgame.game.Drive` object, `from_nflgame` will convert
        `drive` to a `nfldb.Drive` object.

        Generally, this function should not be used. It is called
        automatically by `nfldb.Game.from_nflgame`.
        """
        start_time = _nflgame_clock(drive.time_start)
        start_field = FieldPosition(getattr(drive.field_start, 'offset', None))
        end_field = FieldPosition(drive.field_end.offset)
        end_time = _nflgame_clock(drive.time_end)
        return Drive(g, drive.drive_num, start_field, start_time,
                     end_field, end_time, Team(drive.team),
                     PossessionTime(drive.pos_time.total_seconds()),
                     drive.first_downs, drive.result, drive.penalty_yds,
                     drive.total_yds, drive.play_cnt)

    def __init__(self, g, drive_id, start_field, start_time,
                 end_field, end_time, pos_team, pos_time,
                 first_downs, result, penalty_yards, yards_gained, play_count):
        self.game = g
        """
        The `nfldb.Game` object that this drive belongs to.
        """
        self.drive_id = drive_id
        """
        The numeric drive identifier for this game. It may be
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
        The team in possession during this drive, represented with
        `nfldb.Team`.
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

    def _save(self, cursor):
        vals = [
            ('gsis_id', self.game.gsis_id),
            ('drive_id', self.drive_id),
            ('start_field', self.start_field),
            ('start_time', self.start_time),
            ('end_field', self.end_field),
            ('end_time', self.end_time),
            ('pos_team', self.pos_team),
            ('pos_time', self.pos_time),
            ('first_downs', self.first_downs),
            ('result', self.result),
            ('penalty_yards', self.penalty_yards),
            ('yards_gained', self.yards_gained),
            ('play_count', self.play_count),
        ]
        _upsert(cursor, 'drive', vals, vals[0:2])


class Game (object):
    __slots__ = ['gsis_id', 'gamekey', 'start_time', 'week', 'day_of_week',
                 'season_year', 'season_type',
                 'home_team', 'home_score', 'home_score_q1', 'home_score_q2',
                 'home_score_q3', 'home_score_q4', 'home_score_q5',
                 'home_turnovers',
                 'away_team', 'away_score', 'away_score_q1', 'away_score_q2',
                 'away_score_q3', 'away_score_q4', 'away_score_q5',
                 'away_turnovers',
                 'db',
                 '__drives',
                 ]

    @staticmethod
    def from_nflgame(db, g):
        """
        Converts a `nflgame.game.Game` object to a `nfldb.Game`
        object.

        `db` should be a psycopg2 connection returned by
        `nfldb.connect`.
        """
        home_team = nflgame.standard_team(g.home)
        away_team = nflgame.standard_team(g.away)
        season_type = Enums._nflgame_season_phase[g.schedule['season_type']]
        day_of_week = Enums._nflgame_game_day[g.schedule['wday']]
        start_time = _nflgame_start_time(g)
        game = Game(db, g.eid, g.gamekey, start_time, g.schedule['week'],
                    day_of_week, g.schedule['year'], season_type,
                    home_team, g.score_home, g.score_home_q1,
                    g.score_home_q2, g.score_home_q3, g.score_home_q4,
                    g.score_home_q5, int(g.data['home']['to']),
                    away_team, g.score_away, g.score_away_q1,
                    g.score_away_q2, g.score_away_q3, g.score_away_q4,
                    g.score_away_q5, int(g.data['away']['to']))

        game.__drives = []
        for drive in g.drives:
            game.__drives.append(Drive.from_nflgame(game, drive))
        return game

    def __init__(self, db, gsis_id, gamekey, start_time, week, day_of_week,
                 season_year, season_type,
                 home_team, home_score, home_score_q1, home_score_q2,
                 home_score_q3, home_score_q4, home_score_q5, home_turnovers,
                 away_team, away_score, away_score_q1, away_score_q2,
                 away_score_q3, away_score_q4, away_score_q5, away_turnovers):
        """
        A basic constructor for making a `nfldb.Game` object. It is
        advisable to use one of the `nfldb.Game.from_` methods to
        introduce a new `nfldb.Game` object.
        """

        self.db = db
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
        self.home_team = home_team
        """The team abbreviation for the home team."""
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
        """The team abbreviation for the away team."""
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

    def _save(self, cursor):
        vals = [
            ('gsis_id', self.gsis_id),
            ('gamekey', self.gamekey),
            ('start_time', self.start_time),
            ('week', self.week),
            ('day_of_week', self.day_of_week),
            ('season_year', self.season_year),
            ('season_type', self.season_type),
            ('home_team', self.home_team),
            ('home_score', self.home_score),
            ('home_score_q1', self.home_score_q1),
            ('home_score_q2', self.home_score_q2),
            ('home_score_q3', self.home_score_q3),
            ('home_score_q4', self.home_score_q4),
            ('home_score_q5', self.home_score_q5),
            ('home_turnovers', self.home_turnovers),
            ('away_team', self.away_team),
            ('away_score', self.away_score),
            ('away_score_q1', self.away_score_q1),
            ('away_score_q2', self.away_score_q2),
            ('away_score_q3', self.away_score_q3),
            ('away_score_q4', self.away_score_q4),
            ('away_score_q5', self.away_score_q5),
            ('away_turnovers', self.away_turnovers),
        ]
        _upsert(cursor, 'game', vals, [vals[0]])
        for drive in self.__drives:
            drive._save(cursor)


def _upsert(cursor, table, data, pk):
    update_set = ', '.join(['%s = %s' % (k, '%s') for k, _ in data])
    insert_fields = ', '.join([k for k, _ in data])
    insert_places = ', '.join(['%s' for _ in data])
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
