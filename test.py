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
q.games(team='NE', season_type='Regular', season_year=2012, week=1)
ps = q.as_drives()

print len(ps)
for p in ps: print p.result


