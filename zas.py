#!/usr/bin/env python3
# encoding: utf-8

"""
Usage:
  {cmd} list <filesystem>... [--keep=<time>...] [options]
  {cmd} manage <filesystem>... --keep=<time>... [--exclude=<filter>...] [--run] [options]

Commands:
  list                      List the filesystems. Preview what "manage" would do.
  manage                    Create and delete snapshots to hold one for all required times.

Options:
  <filesystem>              Regular expression which filesystem to snapshot.
  --keep=<time>, -k <time>  Definition how old a snapshot is required to be maximal..
                            {time_help}
  --run, -r                 Really change filesystem, without this do change anything.

Manage options:
  --exclude=<filter>        Regular expression to exclude filesystems by name.
  --prefix=<string>         Snapshot name prefix. [default: snapshot-from-].
  --no-prefix-check         Don't ignore snapshots without matching prefix.
  --symlinks                Create symlink required by samba vfs objects shadow_copy.

Other options:
  --zfs-binary=<path>       Alternative location of zfs binary [default: /sbin/zfs].
  --lock-file=<path>        Alternative location of lock file (default is the script file).
  --logfile=<path>          Write output to logfile (default is STDOUT).
  --verbose, -v             Activate more verbose logging.

Example:

    - Keep snapshots of filesystem tank/backup, that are not older than
      1 through 6 hours, 1 through 7 days and each quarter of a year:

        $ {cmd} manage tank/backup --keep=1H*6,1d*7,1y/4 -r
 """

__author__ = "Frank Epperlein"
__license__ = "MIT"

import subprocess
import re
import sys
import time
import datetime
import os
import itertools
import logging
from textwrap import indent

import docopt


class TimeParser(object):
    """
    Each definition can consist of combinations of:
        a number (1,2,3,...)
        a letter (M,H,d,W,m or y)
            M = Minutes
            H = Hours
            d = Days
            W = Weeks
            m = Month
            y = Years
        an / combined with a number (n) to set any time
            that is an possible multiple of the
            time by n
        an * combined with a number (n) to set the this
            definition for n times

    Multiple definitions should be split by comma (,).

    Examples:
        2h/2;1d/4: keep a version younger then 1h, 2h, 6h,
            12h, 18h and 24h
  
        2h30min/5,1y/12: keep a version younger then 30m,
            60m, 90m, 120m, 180m and each month of a year

        1d*7: keep version for each day of the week
    """

    class Lex(object):

        pattern = [".*", ]
        value = None
        groups = None

        def match(self, text):
            for single in self.pattern:
                match = re.match(r"^(%s).*" % single, text)
                if match:
                    self.value = match.group(1)
                    self.groups = match.groups()
                    return self
            return False

        def __repr__(self):
            return "%s(%s)" % (self.__class__.__name__, self.value)

    class Time(Lex):
        pattern = [r"(\d+)(H|M|d|W|m|y)", ]

        multiplier = {
            'M': 60,
            'H': 60 * 60,
            'd': 60 * 60 * 24,
            'W': 60 * 60 * 24 * 7,
            'm': 60 * 60 * 24 * 30,
            'y': 60 * 60 * 24 * 360
        }

        def enumerate(self):
            return int(self.groups[1]) * self.multiplier.get(self.groups[2], 0)

    class Divider(Lex):
        pattern = [r"/(\d+)", ]

        def enumerate(self, times):
            for this_time in times:
                split = this_time // int(self.groups[1])
                while this_time > 0:
                    yield this_time
                    this_time -= split

    class Multiplier(Lex):
        pattern = [r"\*(\d+)", ]

        def enumerate(self, times):
            for this_time in times:
                for factor in range(1, int(self.groups[1]) + 1):
                    yield this_time * factor

    class Splitter(Lex):
        pattern = [r"[;,]", ]

    class Combination(list):

        def enumerate(self):
            result = []
            for part in self:
                if isinstance(part, TimeParser.Time):
                    result = [sum(result + [part.enumerate()])]
                elif isinstance(part, TimeParser.Divider):
                    result = list(part.enumerate(result))
                elif isinstance(part, TimeParser.Multiplier):
                    result = list(part.enumerate(result))
            return result

    tokens = [
        Time,
        Splitter,
        Divider,
        Multiplier
    ]

    combinations = [
        Combination([Time, Multiplier]),
        Combination([Time, Divider]),
        Combination([Time, ]),
    ]

    def __init__(self, text):
        tokens = self.lex(text)
        combinations = self.combine(tokens)
        self.times = self.enumerate(combinations)
        self.human_times = self.humanize(self.times)

    def lex(self, text):
        while len(text):
            match = False
            for ref in self.tokens:
                match = ref().match(text)
                if match:
                    yield match
                    text = text[len(match.value):]
                    break
            if not match:
                text = text[1:]

    def combine(self, tokens):
        tokens = list(tokens)
        while len(tokens):
            token_index = 0
            for combination in self.combinations:
                for lex_index, lex in enumerate(combination):
                    token_count = 0
                    while token_index < len(tokens):
                        if isinstance(tokens[token_index], lex):
                            token_index += 1
                            token_count += 1
                        else:
                            break
                    if not token_count > 0:
                        token_index = 0
                if token_index > 0:
                    yield self.Combination(tokens[:token_index])
                    break
            tokens = tokens[token_index or 1:]

    @staticmethod
    def enumerate(combinations):
        result = list()
        for combination in combinations:
            result += combination.enumerate()
        result = list(set(result))
        result.sort()
        return result

    def humanize(self, times):
        result = list()
        for this_time in times:
            human_time = self.humanize_time(this_time, join='')
            result.append(human_time)
        return result

    @staticmethod
    def humanize_time(amount, unit="seconds", join=''):
        intervals = [
            1,
            60,
            60 * 60,
            60 * 60 * 24,
            60 * 60 * 24 * 7,
            60 * 60 * 24 * 30,
            60 * 60 * 24 * 360]

        names = [('second', 'seconds'),
                 ('minute', 'minutes'),
                 ('hour', 'hours'),
                 ('day', 'days'),
                 ('week', 'weeks'),
                 ('month', 'months'),
                 ('year', 'years')]

        possible_results = []
        unit = list(map(lambda element: element[1], names)).index(unit)
        amount *= intervals[unit]

        while len(intervals):
            this_result = []
            this_amount = amount
            this_weight = 0
            for name_index in range(len(names) - 1, -1, -1):
                interval_amount = int(this_amount // intervals[name_index])
                if interval_amount > 0:
                    this_result.append((interval_amount, names[name_index][1 % interval_amount]))
                    this_amount -= interval_amount * intervals[name_index]
                    this_weight += interval_amount
            this_weight += len(''.join(map(str, itertools.chain(*this_result))))
            if len(this_result):
                possible_results.append([this_weight, this_result])
            intervals = intervals[:-1]
            names = names[:-1]

        best_result = sorted(possible_results, key=lambda k: k[0])[0][1]

        if join is False:
            return best_result
        else:
            return join.join(map(str, itertools.chain(*best_result)))


class SnapshotManager(object):

    def __init__(self, binary=False, prefix="snapshot-from-", prefix_check=True):
        self.zfs_binary = binary or "/sbin/zfs"
        self.snapshot_prefix = prefix
        self.prefix_check = prefix_check

    class Filesystems(dict):
        pass

    def filesystems(self, includes=None, excludes=None):

        def parse_time(time_str):
            fmt = "%a %b %d %H:%M %Y"
            return datetime.datetime.strptime(time_str, fmt)

        if not excludes:
            excludes = []

        if not includes:
            includes = [".*"]

        ph = subprocess.Popen([self.zfs_binary, "list", "-tall", "-oname,creation,type,mountpoint", "-H"],
                              stdout=subprocess.PIPE)
        now = int(time.time())

        result_index = self.Filesystems()
        for set_record in ph.stdout.readlines():

            set_record = set_record.decode(sys.stdout.encoding)
            set_name, set_creation, set_type, set_mount_point = map(lambda s: s.strip(), set_record.split('\t'))

            if set_type == 'filesystem':
                if any(re.match(inc, set_name) for inc in includes):
                    if not any(re.match(exc, set_name) for exc in excludes):
                        result_index[set_name] = {
                            'creation': parse_time(set_creation),
                            'mount_point': set_mount_point,
                            'snapshots': dict()
                        }

            elif set_type == 'snapshot':
                set_name, snapshot_name = set_name.split('@')

                if self.prefix_check and not snapshot_name.startswith(self.snapshot_prefix):
                    continue

                if set_name in result_index:
                    creation_time = parse_time(set_creation)
                    result_index[set_name]['snapshots'][snapshot_name] = {
                        'creation': parse_time(set_creation),
                        'age': now - int(creation_time.strftime("%s"))}

        return result_index

    class Action:

        def __repr__(self):
            raise NotImplementedError()

        def call(self, *cmd):
            logging.debug("calling %r", ' '.join(cmd))
            ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ps.wait()
            if not ps.returncode:
                logging.info(self)
            else:
                logging.error("%s: %s" % (self, ps.stderr.read().strip()))

        def do(self):
            raise NotImplementedError()

    class CreateSnapshot(Action):

        def __init__(self, manager, filesystem, name):
            self.manager, self.filesystem, self.name = manager, filesystem, name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "create snapshot (%s@%s)" % (self.filesystem, self.name)

        def do(self):
            self.call(self.manager.zfs_binary, "snapshot", "%s@%s" % (self.filesystem, self.name))

    class DeleteSnapshot(Action):

        def __init__(self, manager, filesystem, name):
            self.manager = manager
            self.filesystem = filesystem
            self.name = name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "delete snapshot (%s@%s)" % (self.filesystem, self.name)

        def do(self):
            self.call(self.manager.zfs_binary, "destroy", "%s@%s" % (self.filesystem, self.name))

    class RenameSnapshot(Action):

        def __init__(self, manager, filesystem, old_name, new_name):
            self.manager = manager
            self.filesystem = filesystem
            self.old_name = old_name
            self.new_name = new_name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "rename snapshot (%s@%s > %s)" % (self.filesystem, self.old_name, self.new_name)

        def do(self):
            self.call(self.manager.zfs_binary,
                      "rename",
                      "%s@%s" % (self.filesystem, self.old_name),
                      "%s@%s" % (self.filesystem, self.new_name))

    class CreateSymlink(Action):

        def __init__(self, manager, filesystem, snapshot, mount_point):
            self.manager = manager
            this_filesystem = manager.filesystems(includes=[filesystem])[filesystem]
            if snapshot in this_filesystem['snapshots']:
                creation = this_filesystem['snapshots'][snapshot]['creation']
                self.initialized = True
            else:
                creation = datetime.datetime.now()
                self.initialized = False
            self.link_path = "%s/@GMT-%s" % (mount_point, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            self.snapshot_path = "%s/.zfs/snapshot/%s" % (mount_point, snapshot)
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "create symlink (%s > %s)" % (self.link_path, self.snapshot_path)

        def do(self):

            if not self.initialized:
                return False

            if not os.path.islink(self.link_path) and os.path.isdir(self.snapshot_path):
                try:
                    os.symlink(self.snapshot_path, self.link_path)
                except OSError as e:
                    logging.error("%s: %s" % (self, e))
                else:
                    logging.info(self)

    class DeleteSymlink(Action):

        def __init__(self, mount_point, creation):
            self.link_path = "%s/@GMT-%s" % (mount_point, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "delete symlink (%s)" % self.link_path

        def do(self):
            if os.path.islink(self.link_path):
                try:
                    os.unlink(self.link_path)
                except OSError as e:
                    logging.error("%s: %s" % (self, e))
                else:
                    logging.info(self)

    class RenameSymlink(Action):

        def __init__(self, mount_point, creation, new_name):
            self.link_path = "%s/@GMT-%s" % (mount_point, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            self.new_snapshot_path = "%s/.zfs/snapshot/%s" % (mount_point, new_name)
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "rename symlink (%s > %s)" % (self.link_path, self.new_snapshot_path)

        def do(self):
            try:
                os.unlink(self.link_path)
            except OSError as e:
                logging.error("%s [remove old link]: %s" % (self, e))
            else:
                logging.info(self)

                try:
                    os.symlink(self.new_snapshot_path, self.link_path)
                except OSError as e:
                    logging.error("%s [create new link]: %s" % (self, e))
                else:
                    logging.info(self)

    def _snapshot_name(self, snapshot_creation: datetime.datetime):
        return "%s%s" % (self.snapshot_prefix, snapshot_creation.replace(second=0, microsecond=0).isoformat())

    def plan(self, planed_jobs, planed_filesystems=None, maintain_symlinks=False):

        if planed_filesystems is None:
            planed_filesystems = self.filesystems()

        assert isinstance(planed_jobs, TimeParser)
        assert isinstance(planed_filesystems, self.Filesystems)

        planed_times = sorted(planed_jobs.times)
        for filesystem in planed_filesystems.keys():

            # initialize snapshots
            for snapshot in planed_filesystems[filesystem]['snapshots']:
                planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] = False

            # mark snapshots, we want to keep
            satisfied_jobs = list()
            for job_index, job in enumerate(planed_times):

                if job_index > 0:
                    last_job = planed_times[job_index - 1]
                else:
                    last_job = 0  # 0 = now

                # check which snapshot satisfies which job (in reverse-age order)
                for snapshot in map(lambda k: k[0], sorted(planed_filesystems[filesystem]['snapshots'].items(),
                                                           key=lambda k: k[1]['age'], reverse=True)):

                    age = planed_filesystems[filesystem]['snapshots'][snapshot]['age']
                    if last_job < age <= job:
                        if planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] is False:
                            planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] = job
                            satisfied_jobs.append(job)
                            break

            # remove snapshots we don't need anymore
            for snapshot in planed_filesystems[filesystem]['snapshots']:
                if not planed_filesystems[filesystem]['snapshots'][snapshot]['required_by']:
                    yield self.DeleteSnapshot(self, filesystem, snapshot)
                    if maintain_symlinks:
                        yield self.DeleteSymlink(
                            planed_filesystems[filesystem]['mount_point'],
                            planed_filesystems[filesystem]['snapshots'][snapshot]['creation'])

            # rename snapshots if required (in reverse-age order)
            for snapshot in map(lambda k: k[0], sorted(planed_filesystems[filesystem]['snapshots'].items(),
                                                       key=lambda k: k[1]['age'], reverse=True)):
                if planed_filesystems[filesystem]['snapshots'][snapshot]['required_by']:
                    target_name = self._snapshot_name(planed_filesystems[filesystem]['snapshots'][snapshot]['creation'])
                    if snapshot != target_name:
                        yield self.RenameSnapshot(self, filesystem, snapshot, target_name)
                        if maintain_symlinks:
                            yield self.RenameSymlink(
                                planed_filesystems[filesystem]['mount_point'],
                                planed_filesystems[filesystem]['snapshots'][snapshot]['creation'],
                                target_name)

            # see if we need to add a new snapshot
            if len(planed_times) and planed_times[0] not in satisfied_jobs:
                target_name = self._snapshot_name(datetime.datetime.now())
                yield self.CreateSnapshot(self, filesystem, target_name)
                if maintain_symlinks:
                    yield self.CreateSymlink(
                        self,
                        filesystem,
                        target_name,
                        planed_filesystems[filesystem]['mount_point'])


def print_filesystem_listing(filesystems, plan):

    for fs in filesystems.values():
        fs['actions'] = fs.get('actions', [])

    for action in plan:
        if isinstance(action, SnapshotManager.CreateSnapshot) or \
           isinstance(action, SnapshotManager.DeleteSnapshot) or \
           isinstance(action, SnapshotManager.RenameSnapshot):
            if action.filesystem in filesystems:
                filesystems[action.filesystem]['actions'].append(action)

    for fs_name, fs in sorted(filesystems.items(), key=lambda f: f[0]):

        print()
        print('- filesystem: %s\t creation: %s\t mountpoint: %s' % (fs_name, fs['creation'], fs['mount_point']))

        for snapshot_name, snapshot in sorted(fs['snapshots'].items(), key=lambda s: s[1]['age']):

            ss_action = ''
            for action in fs['actions']:
                if isinstance(action, SnapshotManager.DeleteSnapshot) and action.name == snapshot_name:
                    ss_action = '[DELETE] '
                elif isinstance(action, SnapshotManager.RenameSnapshot) and action.old_name == snapshot_name:
                    ss_action = '[RENAME] '

            print('  %ssnapshot: %s\t creation: %s (age: %s)'
                  % (ss_action,
                     snapshot_name,
                     snapshot['creation'],
                     TimeParser.humanize_time(snapshot['age'], join=' ')))

        for action in fs['actions']:
            if isinstance(action, SnapshotManager.CreateSnapshot):
                print('  [CREATE] %s' % action)

    print()


def lock(path=__file__):
    import fcntl
    import os

    fp = os.open(path, os.O_CREAT | os.O_WRONLY)
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return False
    else:
        return True


def main():
    arguments = docopt.docopt(__doc__.format(cmd=os.path.basename(__file__),
                                             time_help=indent(TimeParser.__doc__, ' ' * 24)))

    logging.basicConfig(
        filename=arguments['--logfile'],
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=arguments['--verbose'] and logging.DEBUG or logging.INFO)

    lock_timeout = 60
    while not lock(arguments['--lock-file'] or __file__):
        if lock_timeout:
            logging.debug('already locked by another process, retry in 1 sec')
            lock_timeout -= 1
            time.sleep(1)
        else:
            logging.critical('already locked by another process, giving up')
            sys.exit(1)

    zsm = SnapshotManager(
        binary=arguments['--zfs-binary'],
        prefix=arguments['--prefix'],
        prefix_check=not arguments['--no-prefix-check'])

    jobs = TimeParser(';'.join(arguments['--keep']))

    try:
        filesystems = zsm.filesystems(includes=arguments['<filesystem>'], excludes=arguments['--exclude'])

    except OSError as oe:
        logging.critical('unable to list filesystems (%s)', oe)

    else:

        logging.debug("planning jobs for following times: %s", ", ".join(jobs.human_times))
        plan = zsm.plan(jobs, filesystems, maintain_symlinks=arguments['--symlinks'])

        if arguments['list']:
            print_filesystem_listing(filesystems, plan if arguments['--keep'] else [])

        elif arguments['manage']:
            for index, action in enumerate(plan):
                if arguments['--run']:
                    assert isinstance(action, SnapshotManager.Action)
                    action.do()


if __name__ == "__main__":
    main()
