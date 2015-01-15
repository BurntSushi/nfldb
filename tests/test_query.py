import pytest

import nfldb


@pytest.fixture
def db():
    return nfldb.connect()


@pytest.fixture
def q(db):
    return nfldb.Query(db).game(season_year=2013, season_type='Regular')


@pytest.fixture
def qgame(q):
    return q.game(gsis_id='2013090800')


def test_num_games_in_season(q):
    assert len(q.as_games()) == (16 * 32) / 2


def test_num_games_in_week(q):
    assert len(q.game(week=1).as_games()) == 16


def test_game_by_team(q):
    q.game(team='NE', week=1)
    assert q.as_games()[0].gsis_id == '2013090800'


def test_limit_no_sort(q):
    q.game(season_year=2013, season_type='Regular', week=1)
    q.limit(1)
    assert len(q.as_games()) == 1


def test_sort(q):
    q.game(season_year=2013, season_type='Regular', week=1)
    q.sort(('passing_yds', 'desc'))
    assert '2013090500' == q.as_plays()[0].drive.game.gsis_id


def test_games_from_player(q):
    q.player(full_name='Tom Brady')
    assert len(q.as_games()) == 16


def test_drives_from_player(qgame):
    qgame.player(full_name='Tom Brady')
    assert len(qgame.as_drives()) == 15


def test_player_position_disjunction(q):
    players = q.player(position=['QB', 'RB', 'WR']).limit(1).as_players()
    assert len(players) == 1


def test_longest_pass_player(q):
    q.sort('passing_yds').limit(1)
    assert q.as_play_players()[0].player.full_name == 'Brandon Weeden'


def test_longest_pass_game_play(q):
    games = q.play(passing_yds=95).as_games()
    assert len(games) == 1
    assert games[0].gsis_id == '2013120101'


def test_game_player_play(q):
    q.player(full_name='Julian Edelman')
    q.play(offense_tds=1)
    assert len(q.as_games()) == 4


def test_player_range_as_plays(q):
    q.player(full_name='Tom Brady')
    q.play_player(passing_yds__ge=40, passing_yds__le=50)
    assert len(q.as_plays()) == 5


def test_player_team_in_game(qgame):
    qgame.player(full_name='Tom Brady').limit(1)
    assert qgame.as_play_players()[0].team == 'NE'


def test_longest_pass_game_play_player(q):
    games = q.play_player(passing_yds=95).as_games()
    assert len(games) == 1
    assert games[0].gsis_id == '2013120101'


def test_lists(q):
    q.game(week=[1, 2, 3])
    assert len(q.as_games()) == 48


def test_disjunctive(db, q):
    big_scores = nfldb.QueryOR(db).game(home_score__ge=55, away_score__ge=55)
    q.andalso(big_scores)
    assert len(q.as_games()) == 2


def test_clock(q):
    end_4th = nfldb.Clock.from_str('Q4', '0:30')
    q.game(week=1).play(passing_cmp=1, time__ge=end_4th)
    assert len(q.as_plays()) == 5


def test_possession_time(q):
    long_drive = nfldb.PossessionTime.from_str('10:00')
    q.drive(pos_time__ge=long_drive)
    assert len(q.as_drives()) == 1


def test_field_position(q):
    own1 = nfldb.FieldPosition.from_str('OPP 1')
    q.drive(start_field__ge=own1)
    assert len(q.as_drives()) == 5


def test_num_first_downs(qgame):
    assert len(qgame.play(pos_team='NE', first_down__ge=1).as_plays()) == 26


def test_play_sort_points(qgame):
    qgame.play(pos_team='NE', points__ge=1).sort('points')
    assert all(p.points > 0 for p in qgame.as_plays())


def test_play_sort_points_other(qgame):
    qgame.play(pos_team='NE', points__ge=1).sort(['points', 'down'])
    assert all(p.points > 0 for p in qgame.as_plays())


def test_play_sort_points_other_limit(db, qgame):
    g = qgame.as_games()[0]
    qscoring = nfldb.Query(db).game(gsis_id=g.gsis_id)
    scoring_plays = qscoring.play(points__ge=1).as_plays()

    qgame.sort(['points']).limit(len(scoring_plays))
    plays = qgame.as_plays()
    assert all([p.points > 0 for p in plays])


def test_aggregate_limit(q):
    q.play(third_down_att=1)
    q.sort('passing_yds').limit(1)
    pp = q.as_aggregate()[0]
    assert pp.player.full_name == 'Matthew Stafford' and pp.passing_yds == 1398


def test_aggregate_filter(q):
    pps = q.aggregate(fumbles_lost__ge=6).as_aggregate()
    assert len(pps) == 1
    assert pps[0].player_id is not None
    assert pps[0].player.full_name == 'Peyton Manning'


def test_play_team(qgame):
    # This demonstrates a breaking change with the old Query API.
    # Namely, the `play` method would accept columns on the `play` table
    # or the `play_player` table.
    with pytest.raises(AssertionError):
        qgame.play(team='NE')


def test_fill_drive(db):
    q = nfldb.Query(db)
    q.drive(gsis_id='2013090800')
    drives = q.as_drives()
    nfldb.Drive.fill_games(db, drives)
    for d in drives:
        assert d._game is not None


def test_fill_play(db):
    q = nfldb.Query(db)
    q.play(gsis_id='2013090800')
    plays = q.as_plays()
    nfldb.Play.fill_drives(db, plays)
    for p in plays:
        assert p._drive is not None
        assert p._drive._game is not None


def test_fill_play_player(db):
    q = nfldb.Query(db)
    q.play_player(gsis_id='2013090800')
    pps = q.as_play_players()
    nfldb.PlayPlayer.fill_plays(db, pps)
    for pp in pps:
        assert pp._play is not None
        assert pp._play._drive is not None
        assert pp._play._drive._game is not None
