from nfldb.team import standard_team


def test_standard_team():
    assert standard_team(None) == 'UNK'
    assert standard_team('') == 'UNK'
    assert standard_team('new york') == 'UNK'
    assert standard_team('New England') == 'NE'
    assert standard_team('JAC') == 'JAC'
    assert standard_team('JAX') == 'JAC'
    assert standard_team('St. Louis') == 'STL'
    assert standard_team('Los Angeles') == 'LA'
    assert standard_team('Rams') == 'LA'
