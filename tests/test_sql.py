import re

import pytest


def join(ent_from, *ent_tos):
    return ent_from._sql_join_all(ent_tos)


def joins_to(ent_from, *ent_tos):
    sql = join(ent_from, *ent_tos)
    tables = [t for e in ent_tos for t, _ in e._sql_tables['tables']]
    for t in tables:
        assert ('LEFT JOIN %s' % t) in sql, \
            'no join for "%s"' % t
    for m in re.finditer('LEFT JOIN (\S+)', sql):
        assert m.group(1) in tables, \
            'found unexpected join with "%s"' % m.group(1)


def test_joins():
    from nfldb.types import *  # o_0

    assert join(Game) == ''
    assert join(Drive) == ''
    assert join(Play) == ''
    assert join(PlayPlayer) == ''
    assert join(Player) == ''

    joins_to(Game, Drive)
    joins_to(Game, Play)
    joins_to(Game, PlayPlayer)
    joins_to(Game, PlayPlayer, Player)

    joins_to(Drive, Game)
    joins_to(Drive, Play)
    joins_to(Drive, PlayPlayer)

    joins_to(Play, PlayPlayer)
    joins_to(Play, Drive, PlayPlayer)
    joins_to(Play, Game, Drive, PlayPlayer)
    joins_to(Play, Game, Drive, PlayPlayer, Player)
    joins_to(Play, PlayPlayer)

    joins_to(PlayPlayer, Game)
    joins_to(PlayPlayer, Drive)
    joins_to(PlayPlayer, Play)
    joins_to(PlayPlayer, Game, Drive)
    joins_to(PlayPlayer, Game, Drive, Play)
    joins_to(PlayPlayer, Game, Drive, Play, Player)


def test_player_joins():
    from nfldb.types import *  # o_0

    # The `player` table can ONLY join with `play_player`. Everything else
    # should fail.
    # N.B. This is a special case handled in nfldb.Query. (If it arises, the
    # `play_player` table is added as a bridge between tables.)
    joins_to(Player, PlayPlayer)
    joins_to(PlayPlayer, Player)
    with pytest.raises(AssertionError):
        joins_to(Game, Player)
    with pytest.raises(AssertionError):
        joins_to(Drive, Player)
    with pytest.raises(AssertionError):
        joins_to(Play, Player)
