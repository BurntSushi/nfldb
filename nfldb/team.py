# This module has a couple pieces duplicated from nflgame. I'd like to have
# a single point of truth, but I don't want to import nflgame outside of
# the update script.

teams = [
    ['ARI', 'Arizona', 'Cardinals', 'NFC', 'West', 'Arizona Cardinals'],
    ['ATL', 'Atlanta', 'Falcons', 'NFC', 'South', 'Atlanta Falcons'],
    ['BAL', 'Baltimore', 'Ravens', 'AFC', 'North', 'Baltimore Ravens'],
    ['BUF', 'Buffalo', 'Bills', 'AFC', 'East', 'Buffalo Bills'],
    ['CAR', 'Carolina', 'Panthers', 'NFC', 'South', 'Carolina Panthers'],
    ['CHI', 'Chicago', 'Bears', 'NFC', 'North', 'Chicago Bears'],
    ['CIN', 'Cincinnati', 'Bengals', 'AFC', 'North', 'Cincinnati Bengals'],
    ['CLE', 'Cleveland', 'Browns', 'AFC', 'North', 'Cleveland Browns'],
    ['DAL', 'Dallas', 'Cowboys', 'NFC', 'East', 'Dallas Cowboys'],
    ['DEN', 'Denver', 'Broncos', 'AFC', 'West', 'Denver Broncos'],
    ['DET', 'Detroit', 'Lions', 'NFC', 'North', 'Detroit Lions'],
    ['GB', 'Green Bay', 'Packers', 'NFC', 'North', 'Green Bay Packers', 'G.B.', 'GNB'],
    ['HOU', 'Houston', 'Texans', 'AFC', 'South', 'Houston Texans'],
    ['IND', 'Indianapolis', 'Colts', 'AFC', 'South', 'Indianapolis Colts'],
    ['JAC', 'Jacksonville', 'Jaguars', 'AFC', 'South', 'Jacksonville Jaguars', 'JAX'],
    ['KC', 'Kansas City', 'Chiefs', 'AFC', 'West', 'Kansas City Chiefs', 'K.C.', 'KAN'],
    ['MIA', 'Miami', 'Dolphins', 'AFC', 'East', 'Miami Dolphins'],
    ['MIN', 'Minnesota', 'Vikings', 'NFC', 'North', 'Minnesota Vikings'],
    ['NE', 'New England', 'Patriots', 'AFC', 'East', 'New England Patriots', 'N.E.', 'NWE'],
    ['NO', 'New Orleans', 'Saints', 'NFC', 'South', 'New Orleans Saints', 'N.O.', 'NOR'],
    ['NYG', 'New York', 'Giants', 'NFC', 'East', 'New York Giants', 'N.Y.G.'],
    ['NYJ', 'New York', 'Jets', 'AFC', 'East', 'New York Jets', 'N.Y.J.'],
    ['OAK', 'Oakland', 'Raiders', 'AFC', 'West', 'Oakland Raiders'],
    ['PHI', 'Philadelphia', 'Eagles', 'NFC', 'East', 'Philadelphia Eagles'],
    ['PIT', 'Pittsburgh', 'Steelers', 'AFC', 'North', 'Pittsburgh Steelers'],
    ['SD', 'San Diego', 'Chargers', 'AFC', 'West', 'San Diego Chargers', 'S.D.', 'SDG'],
    ['SEA', 'Seattle', 'Seahawks', 'NFC', 'West', 'Seattle Seahawks'],
    ['SF', 'San Francisco', '49ers', 'NFC', 'West', 'San Francisco 49ers', 'S.F.', 'SFO'],
    ['STL', 'St. Louis', 'Rams', 'NFC', 'West', 'St. Louis Rams', 'S.T.L.'],
    ['TB', 'Tampa Bay', 'Buccaneers', 'NFC', 'South', 'Tampa Bay Buccaneers', 'T.B.', 'TAM'],
    ['TEN', 'Tennessee', 'Titans', 'AFC', 'South', 'Tennessee Titans'],
    ['WAS', 'Washington', 'Redskins', 'NFC', 'East', 'Washington Redskins', 'WSH'],
    ['UNK', 'UNK', 'UNK', 'UNK', 'UNK'],
]


def standard_team(team):
    """
    Returns a standard abbreviation when team corresponds to a team
    known by nfldb (case insensitive). If no team can be found, then
    `"UNK"` is returned.
    """
    if not team or team.lower == 'new york':
        return 'UNK'

    team = team.lower()
    for variants in teams:
        for variant in variants:
            if team == variant.lower():
                return variants[0]
    return 'UNK'
