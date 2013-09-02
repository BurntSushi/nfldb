import nflgame

# games = nflgame.games_gen(2012, 1, kind='REG') 
games = nflgame.games_gen(2012, kind='REG')
ps = []
for g in games:
    for d in g.drives:
        for p in d.plays:
            ps.append(p)

print len(ps)
print sum(len(getattr(p, '_Play__players')) for p in ps)

for i in xrange(10):
    print ps[i]

