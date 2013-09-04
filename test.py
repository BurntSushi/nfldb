import nfldb

db = nfldb.connect()

# players = nfldb.Query(db, orelse=True) 
# players = players.players(full_name='Tom Brady') 

# q = nfldb.Query(db) 
# q = q.games(team='NE', season_type='Regular', season_year='2012') 
# q = q.drives(pos_time__gt=nfldb.PossessionTime.clock('1:00')) 
# q = q.andalso(players) 
# q = q.andalso(nfldb.Query(db, True).plays(fourth_down_conv=1, fourth_down_failed=1)) 
# games = q.as_games() 
# print len(games) 
# for g in games: 
    # print g.gsis_id, g 

# q.games(season_type='Regular', season_year=2012) 
# q.drives(pos_time__ge=nfldb.PossessionTime.clock('11:00')) 
# q.players(team='NE') 

# games = q.as_plays(sortby='passing_yds', limit=10) 
# games = q.plays(offense_tds__ge=1) 
# games = q.plays(offense_yds__ge=80, third_down_att=1) 
# games = q.as_play_players() 

q = nfldb.Query(db)
q.games(season_type='Regular')
q.players(position='QB')
pps = q.as_aggregate(sortby='passing_tds', limit=10)
for pp in pps: print pp.player, pp.passing_yds, pp.passing_att, pp.passing_tds

# q = nfldb.Query(db) 
# q.games(season_type='Regular', season_year=2012) 
# q.players(full_name='Drew Brees') 
# plays = q.as_plays() 
#  
# print(len(plays)) 
# yds = 0 
# for p in plays: 
    # yds += p.passing_yds 
# print(yds) 

# for g in games: print g, g.offense_yds 

# print '%s' % map(str, q.as_players()) 

# q.plays(passing_yds__ge=5) 
# q.players(position='QB') 
# q.plays(passing_yds__ge=1) 

# q.drives(pos_team='NE', result='Touchdown') 
# q.plays(passing_tds__ge=1) 

# q.plays(passing_yds__ge=40) 
# pps = q.as_plays() 
# print len(pps) 
# for p in pps: print p 

# ps = nfldb.aggregate(q.as_play_players()) 
# for p in ps: print p.player, p 

# print sum(len(p._play_players) for p in ps) 
# for i in xrange(min(10, len(ps))): 
    # print ps[i].pos_time 
# for p in ps: print p.gsis_id, p.drive_id, p.play_id, p.time, p.description 

# g = nfldb.Game.from_id(db, '2012090500') 
# print g 
# for team, player in g.players: 
    # print team, player 
# for d in g.drives: 
    # # print d, len(d.plays), sum(len(p.play_players) for p in d.plays)  
    # for p in d.plays: 
        # for pp in p.play_players: 
            # print pp.player.full_name 

