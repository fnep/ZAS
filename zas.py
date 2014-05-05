#!/usr/bin/env python
# encoding: utf-8
# vim: tabstop=4 shiftwidth=4 smarttab expandtab softtabstop=4 autoindent

__author__ = "Frank Epperlein <zas@shellshark.de>"
__license__ = "MIT"

import subprocess
import re
import sys
import time
import datetime
import os
import itertools
import logging

import docopt


class ParseTime(object):
    """
    Each definition can consist of combinations of:
        a number (1,2,3,...)
        a letter (M,H,d,w,m or y)
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
                split = this_time / int(self.groups[1])
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
                if isinstance(part, ParseTime.Time):
                    result = [sum(result + [part.enumerate()])]
                elif isinstance(part, ParseTime.Divider):
                    result = list(part.enumerate(result))
                elif isinstance(part, ParseTime.Multiplier):
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
        self.human = self.humanize(self.times)

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
        unit = map(lambda element: element[1], names).index(unit)
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


class ParseStart(object):
    """
    Each definition can consist of combinations of:
        a number (1,2,3,...)
        a letter (D,W,H or M)
            d = Day of actual Month
            w = Day of actual Week
            H = Hour of actual Day
            M = Minute of actual Hour
        an optional "*", that means any multiples of the
            defined times
        an + combined with a number to set an offset
            of the defined time to the start of the
            estimated month, week, day or hour
        an .. combined with a number to set a max
            possible value

    Weekdays are defined as:
        1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat, 7=Sun

    Examples:
        5w: the 5th day of the week
        2d*: the 2nd, 4th, 6th, ... day of the month
        2H*+10..14: every second hour in range of 10 to 14
        5M*, every fifth minute
        5w,2d,2H*+10..14,5M*: every fifth minute on every
            second hour in range of 10 to 14 on a day which
            is the second day of the month and the fifth
            day of the week
        5w,14H: 14th hour on each friday
        8H: 8th hour on every day
    """

    def __init__(self, definition, start_time=False):
        """
        Initialize parser.

        :param definition: definition string

        :param start_time: alternative start time
        :type start_time: datetime.datetime or False
        """
        if isinstance(start_time, datetime.datetime):
            self.start_time = start_time
        else:
            self.start_time = datetime.datetime.now()
        self.definition = definition

    @property
    def allowed(self):
        """
        Parse start-time-definition and check if we can
            create new snapshots now.
        """

        def affects(now, reference, multiplier, offset, until):
            offset = offset and int(offset) or 0
            until = until and int(until) or 0

            # not if offset is undershot
            if (now - offset) < 0:
                return False

            # not if until exceeded
            if until and now > until:
                return False

            if multiplier:
                if (now - offset) % reference == 0:
                    return True
            else:
                if (now - offset) == reference:
                    return True

            return False

        defined_criteria = 0

        # day of month
        for match in re.finditer(r"(?P<day>\d+)d(?P<multiplier>\*)?(?:\+(?P<offset>\d+))?(?:\.\.(?P<until>\d+))?",
                                 self.definition):
            defined_criteria += 1
            if not affects(
                    self.start_time.day,
                    int(match.group('day')),
                    match.group('multiplier'),
                    match.group('offset'),
                    match.group('until')):
                return False
            break

        # day of week
        for match in re.finditer(
                r"(?P<weekday>\d+)w(?P<multiplier>\*)?(?:\+(?P<offset>\d+))?(?:\.\.(?P<until>\d+))?",
                self.definition):
            defined_criteria += 1
            if not affects(
                    self.start_time.weekday() + 1,
                    int(match.group('weekday')),
                    match.group('multiplier'),
                    match.group('offset'),
                    match.group('until')):
                return False

            break

        # hour of day
        for match in re.finditer(r"(?P<hour>\d+)H(?P<multiplier>\*)?(?:\+(?P<offset>\d+))?(?:\.\.(?P<until>\d+))?",
                                 self.definition):
            defined_criteria += 1
            if not affects(
                    self.start_time.hour,
                    int(match.group('hour')),
                    match.group('multiplier'),
                    match.group('offset'),
                    match.group('until')):
                return False
            break

        # minute of hour
        for match in re.finditer(r'(?P<hour>\d+)M(?P<multiplier>\*)?(?:\+(?P<offset>\d+))?(?:\.\.(?P<until>\d+))?',
                                 self.definition):
            defined_criteria += 1
            if not affects(
                    self.start_time.minute,
                    int(match.group('hour')),
                    match.group('multiplier'),
                    match.group('offset'),
                    match.group('until')):
                return False
            break

        return bool(defined_criteria)


class SnapshotManager(object):
    def __init__(self, binary=False, prefix="yt", prefix_check=True):

        self.zfs_binary = binary or "/sbin/zfs"
        self.snapshot_prefix = prefix
        self.prefix_check = prefix_check

    class Filesystems(dict):
        pass

    def filesystems(self, includes=None, excludes=None):

        def parse_time(timestr):
            fmt = "%a %b %d %H:%M %Y"
            return datetime.datetime.strptime(timestr, fmt)

        if not excludes:
            excludes = []

        if not includes:
            includes = [".*"]

        ph = subprocess.Popen([self.zfs_binary, "list", "-tall", "-oname,creation,type,mountpoint", "-H"],
                              stdout=subprocess.PIPE)
        now = int(time.time())

        result_index = self.Filesystems()
        for set_record in ph.stdout.readlines():

            set_name = set_record.split('\t')[0].strip()
            set_creation = set_record.split('\t')[1].strip()
            set_type = set_record.split('\t')[2].strip()
            set_mountpoint = set_record.split('\t')[3].strip()

            if set_type == 'filesystem':
                match = False
                if not match:
                    for include in includes:
                        if re.match(include, set_name):
                            match = True

                if match:
                    for exclude in excludes:
                        if re.match(exclude, set_name):
                            match = False
                            break

                if match:
                    result_index[set_name] = {
                        'creation': parse_time(set_creation),
                        'mountpoint': set_mountpoint,
                        'snapshots': dict()
                    }

            elif set_type == 'snapshot':
                snapshot_name = set_name.split('@')[1]
                set_name = set_name.split('@')[0]

                if self.prefix_check and not snapshot_name.startswith(self.snapshot_prefix):
                    continue

                if set_name in result_index:
                    creation_time = parse_time(set_creation)
                    result_index[set_name]['snapshots'][snapshot_name] = {
                        'creation': parse_time(set_creation),
                        'age': now - int(creation_time.strftime("%s"))
                    }

        return result_index

    class CreateSnapshot(object):

        def __init__(self, manager, filesystem, name):
            self.manager = manager
            self.filesystem = filesystem
            self.name = name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "create snapshot (%s@%s)" % (self.filesystem, self.name)

        def do(self):
            cmd = [
                self.manager.zfs_binary,
                "snapshot", "%s@%s" % (self.filesystem, self.name)
            ]
            logging.debug("calling \"%s\"", ' '.join(cmd))
            ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ps.wait()
            if not ps.returncode:
                logging.info(self)
            else:
                logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    class DeleteSnapshot(object):

        def __init__(self, manager, filesystem, name):
            self.manager = manager
            self.filesystem = filesystem
            self.name = name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "delete snapshot (%s@%s)" % (self.filesystem, self.name)

        def do(self):
            cmd = [
                self.manager.zfs_binary,
                "destroy", "%s@%s" % (self.filesystem, self.name)
            ]
            logging.debug("calling \"%s\"", ' '.join(cmd))
            ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ps.wait()
            if not ps.returncode:
                logging.info(self)
            else:
                logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    class RenameSnapshot(object):

        def __init__(self, manager, filesystem, old_name, new_name):
            self.manager = manager
            self.filesystem = filesystem
            self.old_name = old_name
            self.new_name = new_name
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "rename snapshot (%s@%s > %s)" % (self.filesystem, self.old_name, self.new_name)

        def do(self):
            cmd = [
                self.manager.zfs_binary,
                "rename",
                "%s@%s" % (self.filesystem, self.old_name),
                "%s@%s" % (self.filesystem, self.new_name),
            ]
            logging.debug("calling \"%s\"", ' '.join(cmd))
            ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ps.wait()
            if not ps.returncode:
                logging.info(self)
            else:
                logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    class CreateSymlink(object):

        def __init__(self, manager, filesystem, snapshot, mountpoint):
            self.manager = manager
            this_filesystem = manager.filesystems(includes=[filesystem])[filesystem]
            if snapshot in this_filesystem['snapshots']:
                creation = this_filesystem['snapshots'][snapshot]['creation']
                self.initialized = True
            else:
                creation = datetime.datetime.now()
                self.initialized = False
            self.link_path = "%s/@GMT-%s" % (mountpoint, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            self.snapshot_path = "%s/.zfs/snapshot/%s" % (mountpoint, snapshot)
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "create symlink (%s > %s)" % (self.link_path, self.snapshot_path)

        def do(self):

            if not self.initialized:
                return False

            if not os.path.islink(self.link_path) and os.path.isdir(self.snapshot_path):
                cmd = [
                    "ln",
                    "--symbolic",
                    self.snapshot_path,
                    self.link_path
                ]
                logging.debug("calling \"%s\"", ' '.join(cmd))
                ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                ps.wait()
                if not ps.returncode:
                    logging.info(self)
                else:
                    logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    class DeleteSymlink(object):

        def __init__(self, mountpoint, creation):
            self.link_path = "%s/@GMT-%s" % (mountpoint, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "delete symlink (%s)" % self.link_path

        def do(self):
            if os.path.islink(self.link_path):
                cmd = [
                    "rm",
                    self.link_path
                ]
                logging.debug("calling \"%s\"", ' '.join(cmd))
                ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                ps.wait()
                if not ps.returncode:
                    logging.info(self)
                else:
                    logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    class RenameSymlink(object):

        def __init__(self, mountpoint, creation, new_name):
            self.link_path = "%s/@GMT-%s" % (mountpoint, creation.strftime('%Y.%m.%d-%H.%M.%S'))
            self.new_snapshot_path = "%s/.zfs/snapshot/%s" % (mountpoint, new_name)
            logging.debug("planed to %s" % self)

        def __repr__(self):
            return "rename symlink (%s > %s)" % (self.link_path, self.new_snapshot_path)

        def do(self):

            if os.path.islink(self.link_path):
                cmd = ["rm", self.link_path]
                logging.debug("calling \"%s\"", ' '.join(cmd))
                ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                ps.wait()
                if ps.returncode:
                    logging.error("%s: %s" % (self, ps.stderr.read().strip()))

            if os.path.isdir(self.new_snapshot_path):
                cmd = ["ln", "--symbolic", self.new_snapshot_path, self.link_path]
                logging.debug("calling \"%s\"", ' '.join(cmd))
                ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                ps.wait()
                if not ps.returncode:
                    logging.info(self)
                else:
                    logging.error("%s: %s" % (self, ps.stderr.read().strip()))

    def __new_snapshot_name(self, job_time):
        return "%s%s" % (self.snapshot_prefix, ParseTime.humanize_time(job_time, join=""))

    def plan(self,
             planed_jobs,
             planed_filesystems=None,
             maintain_symlinks=False,
             allow_create=True,
             allow_rename=True,
             allow_delete=True):

        if planed_filesystems is None:
            planed_filesystems = self.filesystems()

        assert isinstance(planed_filesystems, self.Filesystems)

        planed_jobs.sort()
        for filesystem in planed_filesystems.iterkeys():

            # initialize snapshots
            for snapshot in planed_filesystems[filesystem]['snapshots']:
                planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] = False

            # mark snapshots, we want to keep
            satisfied_jobs = list()
            for job_index, job in enumerate(planed_jobs):

                if job_index > 0:
                    last_job = planed_jobs[job_index - 1]
                else:
                    last_job = 0

                if job in satisfied_jobs:
                    #continue
                    pass

                # use snapshot list, sorted by age
                sorted_snapshots = sorted(
                    planed_filesystems[filesystem]['snapshots'].items(),
                    key=lambda k: k[1]['age'])

                sorted_snapshots.reverse()
                for snapshot in map(lambda k: k[0], sorted_snapshots):
                    age = planed_filesystems[filesystem]['snapshots'][snapshot]['age']
                    if last_job < age <= job:
                        if planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] is False:
                            planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'] = job
                            satisfied_jobs.append(job)
                            break

            # remove snapshots we don't need anymore
            for snapshot in planed_filesystems[filesystem]['snapshots']:
                if allow_delete and not planed_filesystems[filesystem]['snapshots'][snapshot]['required_by']:
                    yield self.DeleteSnapshot(self, filesystem, snapshot)
                    if maintain_symlinks:
                        yield self.DeleteSymlink(
                            planed_filesystems[filesystem]['mountpoint'],
                            planed_filesystems[filesystem]['snapshots'][snapshot]['creation']
                        )

            # rename the others (in reverse-age order)
            sorted_snapshots = sorted(planed_filesystems[filesystem]['snapshots'].items(), key=lambda k: k[1]['age'])
            sorted_snapshots.reverse()
            for snapshot in map(lambda k: k[0], sorted_snapshots):
                if planed_filesystems[filesystem]['snapshots'][snapshot]['required_by']:
                    target_name = self.__new_snapshot_name(
                        planed_filesystems[filesystem]['snapshots'][snapshot]['required_by'])
                    if allow_rename and snapshot != target_name:
                        yield self.RenameSnapshot(self, filesystem, snapshot, target_name)
                        if maintain_symlinks:
                            yield self.RenameSymlink(
                                planed_filesystems[filesystem]['mountpoint'],
                                planed_filesystems[filesystem]['snapshots'][snapshot]['creation'],
                                target_name
                            )

            # see if we need to an a new snapshot
            if allow_create and len(planed_jobs) and planed_jobs[0] not in satisfied_jobs:
                target_name = self.__new_snapshot_name(planed_jobs[0])
                yield self.CreateSnapshot(self, filesystem, target_name)
                if maintain_symlinks:
                    yield self.CreateSymlink(
                        self,
                        filesystem,
                        target_name,
                        planed_filesystems[filesystem]['mountpoint']
                    )


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

__doc__ = """
Usage:
  """ + os.path.basename(__file__) + """ --include=<filter>... [--keep=<time>...] [--start=<time>...]
    [--exclude=<filter>...] [--prefix=<string>] [--no-prefix-check] [--logfile=<path>]
    [--lockfile=<path>] [--zfs-binary=<path>] [--symlinks] [--verbose] [--run]

Options:
  --include=<filter>    regular expression which filesytem to snapshot
  --keep=<time>         definition how old a snapshot is required to be maximal
""" + "\n                        ".join(ParseTime.__doc__.split('\n')) + """

  --start=<time>        definition which times to create new snapshots
""" + "\n                        ".join(ParseStart.__doc__.split('\n')) + """
                            For sure, the start time should be lower then the
                            lowest keep time.

  --exclude=<filter>    regular expression to exclude filesystems by name
  --logfile=<path>      write output to logfile (default is STDOUT)
  --prefix=<string>     snapshot name prefix [default: yt]
  --no-prefix-check     don't ignore snapshots without matching prefix
  --lockfile=<path>     alternative location of lockfile (default is the script file)
  --zfs-binary=<path>   alternative location of zfs binary [default: /sbin/zfs]
  --symlinks            create symlink required by samba vfs objects shadow_copy
  --verbose, -v         More verbose logging
  --run, -r             really change filesystem, without this doesnt change anything

Example:
  # """ + os.path.basename(__file__) + """ --include=tank/backup --keep=1H*6,1d*7,1y/4 --start=0M -r
  ... create snapshots of filesystem tank/backup, that are not older then
      1 to 6 hours, 1 to 7 days and each quarter of a year, take new snapshots
      only when the hour begins

"""

if __name__ == "__main__":
    arguments = docopt.docopt(__doc__)

    logging.basicConfig(
        filename=arguments['--logfile'],
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=arguments['--verbose'] and logging.DEBUG or logging.INFO)

    lock_timer = 30
    while not lock(arguments['--lockfile'] or __file__):
        if lock_timer:
            logging.debug('this tool is already locked by another process, waiting 1 sec')
            lock_timer -= 1
            time.sleep(1)
        else:
            logging.critical('this tool is already locked by another process, giving up')
            sys.exit(1)

    zsm = SnapshotManager(
        binary=arguments['--zfs-binary'],
        prefix=arguments['--prefix'],
        prefix_check=not arguments['--no-prefix-check']
    )

    jobs = ParseTime(';'.join(arguments['--keep']))
    filesystems = zsm.filesystems(includes=arguments['--include'], excludes=arguments['--exclude'])

    creation_restricted = False
    if len(arguments['--start']):
        creation_restricted = True
        for allowed_start_time in arguments['--start']:
            if ParseStart(allowed_start_time).allowed:
                logging.debug('creation allowed by start definition "%s"', allowed_start_time)
                creation_restricted = False

    logging.debug("planing jobs for following times: %s", ", ".join(jobs.human))
    plan = zsm.plan(
        jobs.times,
        filesystems,
        maintain_symlinks=arguments['--symlinks'],
        allow_create=not creation_restricted)

    for index, action in enumerate(plan):
        if arguments['--run']:
            action.do()