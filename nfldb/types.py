__pdoc__ = {}


class Game (object):
    __pdoc__['Game.gsis_id'] = \
        """
        The NFL GameCenter id of the game. It is a string
        with 10 characters. The first 8 correspond to the date of the
        game, while the last 2 correspond to an id unique to the week that
        the game was played.
        """

    __pdoc__['Game.gamekey'] = \
        """
        Another unique identifier for a game used by the
        NFL. It is a sequence number represented as a 5 character string.
        The gamekey is specifically used to tie games to other resources,
        like the NFL's content delivery network.
        """

    __pdoc__['Game.start_time'] = \
        """
        A Python datetime object corresponding to the
        start time of the game. The timezone of this value will be
        equivalent to the timezone specified by ``set_timezone`` (which is
        by default set to the value specified in the configuration file).
        """

    __pdoc__['Game.week'] = \
        """
        The week number of this game. It is always relative
        to the phase of the season. Namely, the first week of preseason
        is 1 and so is the first week of the regular season.
        """

    __pdoc__['Game.day_of_week'] = \
        """
        The day of the week this game was played on.
        Possible values correspond to the ``gameday`` enum.
        """

    __pdoc__['Game.season_year'] = \
        """
        The year of the season of this game. This
        does not necessarily match the year that the game was played. For
        example, games played in January 2013 are in season 2012.
        """

    __pdoc__['Game.season_type'] = \
        """
        The phase of the season. e.g., ``Preseason``,
        ``Regular season`` or `Postseason``. All valid values correspond
        to the ``season_phase`` enum.
        """

    __pdoc__['Game.home_team'] = 'The team abbreviation for the home team.'
    __pdoc__['Game.home_score'] = 'The current total score for the home team.'
    __pdoc__['Game.home_score_q1'] = 'The 1st quarter score for the home team.'
    __pdoc__['Game.home_score_q2'] = 'The 2nd quarter score for the home team.'
    __pdoc__['Game.home_score_q3'] = 'The 3rd quarter score for the home team.'
    __pdoc__['Game.home_score_q4'] = 'The 4th quarter score for the home team.'
    __pdoc__['Game.home_score_q5'] = 'The OT quarter score for the home team.'
    __pdoc__['Game.home_turnovers'] = 'Total turnovers for the home team.'

    __pdoc__['Game.away_team'] = 'The team abbreviation for the away team.'
    __pdoc__['Game.away_score'] = 'The current total score for the away team.'
    __pdoc__['Game.away_score_q1'] = 'The 1st quarter score for the away team.'
    __pdoc__['Game.away_score_q2'] = 'The 2nd quarter score for the away team.'
    __pdoc__['Game.away_score_q3'] = 'The 3rd quarter score for the away team.'
    __pdoc__['Game.away_score_q4'] = 'The 4th quarter score for the away team.'
    __pdoc__['Game.away_score_q5'] = 'The OT quarter score for the away team.'
    __pdoc__['Game.away_turnovers'] = 'Total turnovers for the away team.'

    def __init__(self):
        pass
