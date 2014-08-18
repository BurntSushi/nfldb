#!/usr/bin/env python2.7

from __future__ import absolute_import, division, print_function
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
import datetime
import subprocess
import sys
import time

import nfldb

import nflgame
import nflgame.live


_simulate = None
"""
When run in simulation mode, this is set to a dictionary with global
state indicating which games to update and some other state that
indicates how much of the game should be updated.
"""


def log(*args, **kwargs):
    kwargs['file'] = sys.stderr
    print(*args, **kwargs)
    sys.stderr.flush()


def now():
    return datetime.datetime.now()


def seconds_delta(d):
    """
    The same as `datetime.timedelta.total_seconds` in the standard
    library. Defined here for Python 2.6 compatibility.

    `d` should be a `datetime.timedelta` object.
    """
    return (d.microseconds + (d.seconds + d.days * 24 * 3600) * 10**6) / 10**6


def game_from_id(cursor, gsis_id):
    """
    Returns an `nfldb.Game` object given its GSIS identifier.
    Namely, it looks for a completed or in progress game in nflgame's
    schedule, otherwise it creates a dummy `nfldb.Game` object with
    data from the schedule.
    """
    schedule = nflgame.sched.games[gsis_id]
    start_time = nfldb.types._nflgame_start_time(schedule)
    if seconds_delta(start_time - nfldb.now()) >= 900:
        # Bail quickly if the game isn't close to starting yet.
        return game_from_schedule(cursor, gsis_id)

    g = nflgame.game.Game(gsis_id)
    if g is None:  # Whoops. I guess the pregame hasn't started yet?
        return game_from_schedule(cursor, gsis_id)
    return nfldb.Game._from_nflgame(cursor.connection, g)


def game_from_id_simulate(cursor, gsis_id):
    """
    Returns a "simulated" `nfldb.Game` object corresponding to `gsis_id`.
    The data is retrieved from `nflgame` just like in `game_from_id`,
    except the drives are shortened based on the number of simulation
    rounds that have executed.

    If the number of drives is not shortened, then that game's simulation
    is done and it is marked as finished.

    If `gsis_id` corresponds to a game that hasn't already finished, then
    this will raise a `ValueError`.
    """
    g = nflgame.game.Game(gsis_id)
    if g is None or not g.game_over():
        raise ValueError(
            "It looks like '%s' hasn't finished yet, so I cannot simulate it.")

    dbg = nfldb.Game._from_nflgame(cursor.connection, g)
    old_drives = dbg._drives  # filled in from nflgame data
    new_drives = dbg._drives[0:_simulate['drives']]
    if len(old_drives) == len(new_drives):
        _simulate['gsis_ids'].pop(_simulate['gsis_ids'].index(gsis_id))
        log('DONE simulating game "%s".' % gsis_id)
        return dbg
    dbg.finished = False
    dbg._drives = new_drives
    return dbg


def game_from_schedule(cursor, gsis_id):
    """
    Returns an `nfldb.Game` object from schedule data in
    `nflgame.sched`.

    This is useful when you want to avoid initializing a
    `nflgame.game.Game` object.
    """
    s = nflgame.sched.games[gsis_id]
    return nfldb.Game._from_schedule(cursor.connection, s)


def update_season_state(cursor):
    phase_map = nfldb.types.Enums._nflgame_season_phase

    try:
        nflgame.live._update_week_number()
    except:  # Don't let a bad download kill the update script.
        log('FAILED!')
        return
    typ = phase_map[nflgame.live._cur_season_phase]
    cursor.execute('''
        UPDATE meta SET season_type = %s, season_year = %s, week = %s
    ''', (typ, nflgame.live._cur_year, nflgame.live._cur_week))


def run_cmd(*cmd):
    try:
        subprocess.check_call(cmd, stdout=sys.stdout, stderr=sys.stderr)
    except subprocess.CalledProcessError as e:
        log('`%s` failed (exit status %d)' % (' '.join(e.cmd), e.returncode))
    except OSError as e:
        log('`%s` failed [Errno %d]: %s'
            % (' '.join(cmd), e.errno, e.strerror))


def update_players(cursor, interval):
    db = cursor.connection
    cursor.execute('SELECT last_roster_download FROM meta')
    last = cursor.fetchone()['last_roster_download']
    update_due = seconds_delta(nfldb.now() - last) >= interval
    num_existing = nfldb.db._num_rows(cursor, 'player')

    # The interval only applies if the player table has data in it.
    # If it's empty, we always want to try and update regardless of interval.
    if not update_due and num_existing > 0:
        return

    log('Updating player JSON database... (last update was %s)' % last)
    run_cmd(sys.executable, '-m', 'nflgame.update_players', '--no-block')
    log('done.')

    # Reset the player JSON database.
    nflgame.players = nflgame.player._create_players()

    log('Locking player table...')
    cursor.execute('''
        LOCK TABLE player IN SHARE ROW EXCLUSIVE MODE
    ''')

    log('Updating %d players... ' % len(nflgame.players), end='')
    for p in nflgame.players.itervalues():
        dbp = nfldb.Player._from_nflgame_player(db, p)
        for table, prim, vals in dbp._rows:
            nfldb.db._upsert(cursor, table, vals, prim)
    log('done.')

    # If the player table is empty at this point, then something is very
    # wrong. The user MUST fix things before going forward.
    if nfldb.db._num_rows(cursor, 'player') == 0:
        log('Something is very wrong. The player table is empty even after\n'
            'trying to update it. Please seek help. Include the output of\n'
            'this program when asking for help.')
        log('The likely cause here is that the `nflgame-update-players`\n'
            'program is failing somehow. Try running it separately to see\n'
            'if it succeeds on its own.')
        sys.exit(1)

    # Finally, report that we've just update the rosters.
    cursor.execute('UPDATE meta SET last_roster_download = NOW()')


def bulk_insert_game_data(cursor, scheduled, batch_size=5):
    """
    Given a list of GSIS identifiers of games that have **only**
    schedule data in the database, perform a bulk insert of all drives
    and plays in the game.
    """
    def do():
        log('\tSending batch of data to database.')
        for table in ('drive', 'play', 'play_player'):  # order matters
            if len(bulk.get(table, [])) > 0:
                nfldb.db._big_insert(cursor, table, bulk[table])
                bulk[table] = []

    bulk = OrderedDict()
    queued = 0
    for gsis_id in scheduled:
        if queued >= batch_size:
            do()
            queued = 0
        g = game_from_id(cursor, gsis_id)

        # This updates the schedule data to include all game meta data.
        # We don't use _save here, as that would recursively upsert all
        # drive/play data in the game.
        for table, prim, vals in g._rows:
            nfldb.db._upsert(cursor, table, vals, prim)

        queued += 1
        for drive in g.drives:
            for table, prim, vals in drive._rows:
                bulk.setdefault(table, []).append(vals)
            for play in drive.plays:
                for table, prim, vals in play._rows:
                    bulk.setdefault(table, []).append(vals)
                for pp in play.play_players:
                    for table, prim, vals in pp._rows:
                        bulk.setdefault(table, []).append(vals)
                    # Whoops. Shouldn't happen often...
                    # Only inserts into the DB if the player wasn't found
                    # in the JSON database. A few weird corner cases...
                    pp.player._save(cursor)

    # Bulk insert leftovers.
    do()


def games_in_progress(cursor):
    """
    Returns a list of GSIS identifiers corresponding to games that
    are in progress. Namely, they are not finished but have at least
    one drive in the database.

    The list is sorted in the order in which the games will be played.
    """
    playing = []
    cursor.execute('''
        SELECT DISTINCT game.gsis_id, game.finished
        FROM drive
        LEFT JOIN game
        ON drive.gsis_id = game.gsis_id
        WHERE game.finished = False AND drive.drive_id IS NOT NULL
    ''')
    for row in cursor.fetchall():
        playing.append(row['gsis_id'])
    return sorted(playing, key=int)


def games_scheduled(cursor):
    """
    Returns a list of GSIS identifiers corresponding to games that
    have schedule data in the database but don't have any drives or
    plays in the database. In the typical case, this corresponds to
    games that haven't started yet.

    The list is sorted in the order in which the games will be played.
    """
    scheduled = []
    cursor.execute('''
        SELECT DISTINCT game.gsis_id, game.start_time
        FROM game
        LEFT JOIN drive
        ON game.gsis_id = drive.gsis_id
        WHERE drive.drive_id IS NULL
    ''')
    for row in cursor.fetchall():
        # This condition guards against unnecessarily processing games
        # that have only schedule data but aren't even close to starting yet.
        # Namely, if a game doesn't have any drives, then there's nothing to
        # bulk insert.
        #
        # We start looking at games when it's 15 minutes before game time.
        # Eventually, the game will start, and the first bits of drive/play
        # data will be bulk inserted. On the next database update, the game
        # will move to the `games_in_progress` list and updated incrementally.
        #
        # So what's the point of bulk inserting? It's useful when updates are
        # done infrequently (like the initial load of the database or say,
        # once a week).
        if seconds_delta(row['start_time'] - nfldb.now()) < 900:
            scheduled.append(row['gsis_id'])
    return sorted(scheduled, key=int)


def games_missing(cursor):
    """
    Returns a list of GSIS identifiers corresponding to games that
    don't have any data in the database.

    The list is sorted in the order in which the games will be played.
    """
    allids = set()
    cursor.execute('SELECT gsis_id FROM game')
    for row in cursor.fetchall():
        allids.add(row['gsis_id'])
    nada = (gid for gid in nflgame.sched.games if gid not in allids)
    return sorted(nada, key=int)


def update_game_schedules(db):
    """
    Updates the schedule data of every game in the database.
    """
    update_nflgame_schedules()
    log('Updating all game schedules... ', end='')
    with nfldb.Tx(db) as cursor:
        lock_tables(cursor)
        for gsis_id in nflgame.sched.games:
            g = game_from_id(cursor, gsis_id)
            for table, prim, vals in g._rows:
                nfldb.db._upsert(cursor, table, vals, prim)
    log('done.')


def update_current_week_schedule(db):
    update_nflgame_schedules()

    phase_map = nfldb.types.Enums._nflgame_season_phase
    phase, year, week = nfldb.current(db)
    log('Updating schedule for (%s, %d, %d)' % (phase, year, week))
    with nfldb.Tx(db) as cursor:
        for gsis_id, info in nflgame.sched.games.iteritems():
            if year == info['year'] and week == info['week'] \
                    and phase == phase_map[info['season_type']]:
                g = game_from_id(cursor, gsis_id)
                for table, prim, vals in g._rows:
                    nfldb.db._upsert(cursor, table, vals, prim)
    log('done.')


def update_nflgame_schedules():
    log('Updating schedule JSON database...')
    run_cmd(sys.executable, '-m', 'nflgame.update_sched')
    log('done.')


def update_games(db, batch_size=5):
    """
    Does a single monolithic update of players, games, drives and
    plays.  If `update` terminates, then the database will be
    completely up to date with all current NFL data known by `nflgame`.

    Note that while `update` is executing, all writes to the following
    tables will be blocked: player, game, drive, play, play_player.
    The huge lock is used so that there aren't any races introduced
    when updating the database. Other clients will still be able to
    read from the database.
    """
    # The complexity of this function has one obvious culprit:
    # performance reasons. On the one hand, we want to make infrequent
    # updates quick by bulk-inserting game, drive and play data. On the
    # other hand, we need to be able to support incremental updates
    # as games are played.
    #
    # Therefore, games and their data are split up into three chunks.
    #
    # The first chunk are games that don't exist in the database at all.
    # The games have their *schedule* data bulk-inserted as a place holder
    # in the `game` table. This results in all of the `home_*` and `away_*`
    # fields being set to 0. The schedule data is bulk inserted without
    # ever initializing a `nflgame.game.Game` object, which can be costly.
    #
    # The second chunk are games that have schedule data in the database
    # but have nothing else. In the real world, this corresponds to games
    # in the current season that haven't started yet. Or more usefully,
    # every game when the database is empty. This chunk of games has its
    # drives and play bulk-inserted.
    #
    # The third and final chunk are games that are being played. These games
    # have the slowest update procedure since each drive and play need to be
    # "upserted." That is, inserted if it doesn't exist or updated if it
    # does. On the scale of a few games, performance should be reasonable.
    # (Data needs to be updated because mistakes can be made on the fly and
    # corrected by the NFL. Blech.)
    #
    # Comparatively, updating players is pretty simple. Player meta data
    # changes infrequently, which means we can update it on a larger interval
    # and we can be less careful about performance.
    with nfldb.Tx(db) as cursor:
        lock_tables(cursor)

        log('Updating season phase, year and week... ', end='')
        update_season_state(cursor)
        log('done.')

        nada = games_missing(cursor)
        if len(nada) > 0:
            log('Adding schedule data for %d games... ' % len(nada), end='')
            insert = OrderedDict()
            for gid in nada:
                g = game_from_schedule(cursor, gid)
                for table, prim, vals in g._rows:
                    insert.setdefault(table, []).append(vals)
            for table, vals in insert.items():
                nfldb.db._big_insert(cursor, table, vals)
            log('done.')

        scheduled = games_scheduled(cursor)
        if len(scheduled) > 0:
            log('Bulk inserting data for %d games...' % len(scheduled))
            bulk_insert_game_data(cursor, scheduled, batch_size=batch_size)
            log('done.')

        playing = games_in_progress(cursor)
        if len(playing) > 0:
            log('Updating %d games in progress...' % len(playing))
            for gid in playing:
                g = game_from_id(cursor, gid)
                log('\t%s' % g)
                g._save(cursor)
            log('done.')

        # This *must* come after everything else because it could set
        # the 'finished' flag to true on a game that hasn't been completely
        # updated yet.
        #
        # See issue #42.
        update_current_week_schedule(db)


def update_simulate(db):
    with nfldb.Tx(db) as cursor:
        log('Simulating %d games...' % len(_simulate['gsis_ids']))
        for gid in _simulate['gsis_ids']:
            g = game_from_id_simulate(cursor, gid)
            log('\t%s' % g)
            g._save(cursor)
        log('done.')

        if len(_simulate['gsis_ids']) == 0:
            return True
    _simulate['drives'] += 1
    return False


def lock_tables(cursor):
    log('Locking write access to tables... ', end='')
    cursor.execute('''
        LOCK TABLE player IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE game IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE drive IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE play IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE play_player IN SHARE ROW EXCLUSIVE MODE
    ''')
    log('done.')


def run(player_interval=43200, interval=None, update_schedules=False,
        batch_size=5, simulate=None):
    global _simulate

    if simulate is not None:
        assert not update_schedules, \
            "update_schedules is incompatible with simulate"

        db = nfldb.connect()

        # Expand `simulate` to a real list of gsis ids since prefixes
        # are allowed.
        lt = [gid + ('\x79' * (10 - len(gid))) for gid in simulate]
        q = nfldb.Query(db).game(gsis_id__ge=simulate, gsis_id__le=lt)
        games = sorted(q.as_games(), key=lambda g: g.gsis_id)
        for g in games:
            if not g.finished:
                log('Game "%s" has not finished yet and therefore cannot '
                    'be simulated.' % g.gsis_id)
                sys.exit(1)
        simulate = [g.gsis_id for g in games]

        yesno = raw_input(
            '*** PLEASE READ! ***\n\n'
            'Simulation mode will simulate games being played by deleting\n'
            'games from the database and slowly re-adding drives in the game\n'
            'one-by-one at a constant rate indicated by --interval.\n'
            'You may cancel the simulation at any time and run \n'
            '`nfldb-update` to bring the database back up to date.\n\n'
            'Please make sure that no other `nfldb-update` processes are\n'
            'running during a simulation.\n\n'
            '    %s\n\n'
            'Are you sure you want to simulate these games? [y/n] '
            % '\n    '.join(simulate))
        if yesno.strip().lower()[0] != 'y':
            sys.exit(0)

        _simulate = {
            'gsis_ids': simulate,
            'drives': 0,
        }

        log('Running simulation... Deleting games: %s' % ', '.join(simulate))
        with nfldb.Tx(db) as cursor:
            cursor.execute('DELETE FROM game WHERE gsis_id IN %s',
                           (tuple(simulate),))

        if interval is None:
            # Simulation implies a repeated update at some interval.
            interval = 10
            log('--interval not set, so using default simulation '
                'interval of %d seconds.' % interval)

    def doit():
        log('-' * 79)
        log('STARTING NFLDB UPDATE AT %s' % now())

        log('Connecting to nfldb... ', end='')
        db = nfldb.connect()
        log('done.')

        # We always insert dates and times as UTC.
        log('Setting timezone to UTC... ', end='')
        nfldb.set_timezone(db, 'UTC')
        log('done.')

        if update_schedules:
            update_game_schedules(db)
        elif simulate is not None:
            done = update_simulate(db)
            if done:
                log('Simulation complete.')
                return True
        else:
            with nfldb.Tx(db) as cursor:
                # Update players first. This is important because if an unknown
                # player is discovered in the game data, the player will be
                # upserted. We'd like to avoid that because it's slow.
                update_players(cursor, player_interval)

            # Now update games.
            update_games(db, batch_size=batch_size)

        log('Closing database connection... ', end='')
        db.close()
        log('done.')

        log('FINISHED NFLDB UPDATE AT %s' % now())
        log('-' * 79)

    if interval is None:
        doit()
    else:
        if interval < 15 and simulate is None:
            log('WARNING: Interval %d is shorter than 15 seconds and is '
                'probably wasteful.\nAre you sure you know what you are doing?'
                % interval)
        while True:
            done = doit()
            if done:
                sys.exit(0)
            time.sleep(interval)
