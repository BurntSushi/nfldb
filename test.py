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

q = nfldb.Query(db)
# q.games(season_type='Regular', season_year=2012, week=1) 
q.games(season_type='Regular', season_year=2012)
# q.drives(pos_time__ge=nfldb.PossessionTime.clock('11:00')) 
# q.players(full_name='Tom Brady') 
q.plays(rushing_yds__ge=70)
ps = q.as_plays()

print len(ps)
# print sum(len(p._play_players) for p in ps) 
# for i in xrange(min(10, len(ps))): 
    # print ps[i].pos_time 
for p in ps: print p.down, p.description


