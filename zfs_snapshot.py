#!/usr/local/bin/python

import subprocess, re, sys, getopt, time, datetime, types, os

ZFS_BINARY="/sbin/zfs"

def get_snapshots (filesystem):
	global ZFS_BINARY
	ph = subprocess.Popen([ZFS_BINARY, "list", "-tsnapshot", "-H", "-oname", "-sname"], stdout=subprocess.PIPE)
	ph.wait()
	index = []
	for line in ph.stdout:
		parse = re.search("(?P<dataset>[^@]+)@" + 
				  "(?P<snapshot>[^\s]+)\s", 
				   line)
		if parse:
		  if parse.group('dataset') == filesystem:
		    index.append( parse.group('snapshot').rstrip() )
	return index


def get_filesystems (regexfilter=".*", excludes=[]):
	global ZFS_BINARY
	ph = subprocess.Popen([ZFS_BINARY, "list", "-tfilesystem",  "-oname", "-H"], stdout=subprocess.PIPE)
	ph.wait()
	index = []
	for line in ph.stdout:
		parse = re.search("(?P<dataset>[^@]+)", line)
		value = parse.group('dataset').rstrip()
		if parse:
		  if re.match(regexfilter, value):
		    incl = True
		    for exclude in excludes:
                      if re.match(exclude, value): incl = False
		    if incl: index.append( value )
		    else: continue
	return index


def  get_snaplist (regexfilter=".*", excludes=[]):
	index = {}
	for single in get_filesystems(regexfilter, excludes):
		index[single] = get_snapshots(single)
	return index


def new_snapshot_name ():
	fmt = "GMT-%Y.%m.%d-%H.%M.%S"
	return datetime.datetime.now().strftime(fmt)

def parse_snapshot_name (date_string):
	fmt = "GMT-%Y.%m.%d-%H.%M.%S"
	return datetime.datetime.strptime(date_string, fmt)


def parse_keep_jobs (keep_jobs):
	ret = []
	for job in keep_jobs:
	    # replace any unit by ist seconds-equivalent
	    search = re.findall("(\d+)([a-zA-Z]{1})", job)
	    if search:
	      for entry in search:
	        if entry[1] == "y": replacement = int(entry[0]) * 31536000
                elif entry[1] == "M": replacement = int(entry[0]) * 2628000
                elif entry[1] == "d": replacement = int(entry[0]) * 86400
                elif entry[1] == "h": replacement = int(entry[0]) * 3600
                elif entry[1] == "m": replacement = int(entry[0]) * 60
	        else:
	          log("unknown multiplier '%s'" % entry[1])
	          sys.exit(1)
	        job = job.replace("%s%s" % (entry[0], entry[1]), "+%s" % str(replacement), 1)

	    # replace multi-statement-entrys with its evaluation
	    search = re.findall("([\d+ ]+)", job)
	    if search:
	      for entry in search:
	        replacement = eval(entry)
	        job = job.replace(entry, str(replacement), 1)

	    # add adtiotional versions, createt by given devisor
	    search = re.findall("(\d+)/(\d+)", job)
	    if search:
	      for entry in search:
		bwd = int(entry[0]) / int(entry[1]) 
		sum = bwd
	        replacement = ""
		while (sum <= int(entry[0])):
	          replacement += ";" + str(sum)
		  sum += bwd
	        job = job.replace("%s/%s" % (entry[0], entry[1]), str(replacement), 1)
	    for digit in job.split(";"):
	      try:
	        ret.append(int(digit))
	      except Exception, e:
	        pass

	ret = list(set(ret))
	ret.sort()
	return ret


def create_snapshots(index, doit=False):
	global ZFS_BINARY
	log("Creating snapshots, ...", "DEBUG")
	for volume in index:
	    newname = "%s@%s" % (volume, new_snapshot_name())
	    log("creating %s" % newname, "DEBUG")
	    if doit:
	        ph = subprocess.Popen([ZFS_BINARY, "snapshot", newname], stderr=subprocess.PIPE)
	        ph.wait()
	        if ph.returncode == 0:
	    	    log("created snapshot %s" % (newname), "VERBOSE")
	        else:
	            log("can't create snapshot %s (%s)" % (newname, str(ph.stderr.readlines())))

def cleanup_snapshots(index, keep_jobs, doit=False):
	global ZFS_BINARY
	log("Tidying up snapshots, ...", "DEBUG"); 
	request_snap = []
	request_link = []
	for volume in index.iterkeys():
		if isinstance(index[volume], types.ListType):
		  # per volume level
		  keep = []
		  destroy = []
		  keep_jobs.sort()
		  for job in keep_jobs:
		      # per job level
  		      first_job = True
		      to_keep = False
		      for snapshot in index[volume]:
			    # per snapshot level
			    snapshot_time = parse_snapshot_name(snapshot)
			    delta_time = datetime.datetime.fromtimestamp(
					    time.time() - 
					    int(job) )
			    if ( delta_time < snapshot_time ):
			          if ( not to_keep or
			               snapshot_time < parse_snapshot_name(to_keep)):
			               to_keep = snapshot
		      if to_keep: keep.append(to_keep)
		      if first_job == True:
		        first_job == False
		        if not to_keep:
		          log("volume %s, job -%s seconds, requires new snapshot" % (volume, job), "DEBUG")
		          request_snap.append(volume)
		  for snapshot in index[volume]:
			if snapshot not in keep:
				destroy.append(snapshot)
		  keep = list(set(keep))
		  destroy = list(set(destroy))
		  log("%s: keep: %s destroy: %s" % (volume, str(keep), str(destroy)), "DEBUG")
	          if doit:
		    for snapshot in destroy:
		      snapname = "%s@%s" % (volume, snapshot)
	              ph = subprocess.Popen([ZFS_BINARY, "destroy", snapname], stderr=subprocess.PIPE)
	              ph.wait()
	              if ph.returncode == 0:
	    	        log("destroyed snapshot %s@%s" % (volume, snapshot), "VERBOSE")
	              else:
	                log("can't destroy snapshot %s@%s (%s)" % (volume, snapshot, str(ph.stderr.readlines())))
	              if not volume in request_link: request_link.append(volume)
	request_snap = list(set(request_snap))
	return (request_snap, request_link)


def update_symlinks (volumes, doit=False):
	global ZFS_BINARY
	for volume in volumes:
	    ph = subprocess.Popen([ZFS_BINARY, "get", "-ovalue", "-H", "mountpoint", volume], stdout=subprocess.PIPE)
	    ph.wait()
	    mountpoint = (ph.stdout.readline()).rstrip()
	    log("symlink update in %s" % mountpoint.rstrip(), "DEBUG")
	    snapshotpath = ("%s/.zfs/snapshot/" % mountpoint).rstrip()
	    if os.path.isdir(mountpoint):
	        for link in os.listdir(mountpoint):
	            oldlink = "%s/%s" % (mountpoint, link)
	            if os.path.islink(oldlink) and not os.path.isdir(oldlink):
	                if parse_snapshot_name(link.lstrip("@")):
	                    if doit: os.remove(oldlink)
	                    log("symlink removed %s" % (oldlink), "DEBUG")
	    if os.path.isdir(snapshotpath):
	      for snapshot in os.listdir(snapshotpath):
	        source = os.path.realpath("%s/%s" % (snapshotpath, snapshot))
	        target = os.path.abspath("%s/@%s" % (mountpoint, snapshot))
	        if not os.path.islink(target) and parse_snapshot_name(snapshot):
	          if doit: os.symlink(source, target)
	          log("symlink %s -> %s" % (source, target), "DEBUG")

def lockFile(lockfile):
	import fcntl, os
	fp = os.open(lockfile, os.O_CREAT | os.O_TRUNC | os.O_WRONLY)
	try:
	    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
	except IOError:
	    return False
	else:
	    return True


def usage ():
	print """
%s --filter=<filter> [--exclude=<filter>]  [--zfs-binary=<path>]
                     [--keep=<time>] [--create-symlinks] [--verbose] [--run]

  --filter=<filter>      Regulaerer Ausdruck der bestimmt welche 
                         filesystems bearbeitet werden sollen

  --exclude=<filter>     Regulaerer Ausdruck der bestimmt welche 
                         filesystems NICHT bearbeitet werden sollen
                         Kann mehrfach angegeben werden

  --zfs-binary=<path>    Pfad zum zfs-binary, standard: /sbin/zfs

  --keep=<time>          Festlegung welche snapshot-versionen 
                         bewahrt werden sollen. Angaben in sekunden
                         oder mit folgenden Einheiten:
                          m Minuten 
                          h Stunden 
                          d Tage    
                          M Monate  
                          y Jahre
                         
                        Bei angabe eines Teilers (/<zahl>) wird eine
                        weitere Version fuer jeden vielfachen des 
                        Quotienten angenommen. Mehrere Angaben 
                        koennen Semikolon separiert aufgefuehrt 
                        werden. --keep kann mehrfach angegeben 
                        werden.

                        Beispiele:
                        --keep 2h/2;1d/4 entspricht einer Version vor
                         1h, 2h, 6h, 12h, 18h und 24h
                        --keep 2h30min/5 --keep 1y/12 entspricht einer
                         Version vor 30m, 60m, 90m, 120m, 180m und 
                         vor jeweils einem Monat

  --create-symlinks    Legt Symlinks in der Wurzel des filesystem an
                       um samba vfs objects = shadow_copy fuer die
                       MS Windows Previous Versions zu unterstuetzen

  --verbose            Zeigt an welche snapshots angelegt oder 
                       geloescht werden

  --run                Nimmt aenderungen vor, ohne --run werden weder
                       snapshots oder symlinks angelegt oder entfernt

""" % (__file__)


LOGFILTER=0
def log (message, target="ERROR"):
	global LOGFILTER
	if   target.upper().startswith("V"): target = 1
	elif target.upper().startswith("D"): target = 2
	else: target = 0
	fmt = "%Y-%m-%d %H:%M:%S"
	timestamp = datetime.datetime.now().strftime(fmt) 
	message = "%s  %s" % (timestamp, message)
	if target == 0:
	  print message
	elif target == 1:
	  if LOGFILTER >= 1: print message
	elif target == 2:
	  if LOGFILTER >= 2: print message


def main (argv):
	try:
	  opts, args = getopt.getopt(argv, "hf:k:sv", ["help", "filter=", "exclude=", "keep=", "create-symlinks", "verbose", "run", "debug", "zfs-binary="])
	except getopt.GetoptError:
	  usage()
	  sys.exit(2)

	get_filesystem_args = {}
	keep_jobs   = []
	verbose     = False
	doit        = False
	symlinks    = False
	debug       = False
	regexfilter = False
	excludes    = []
	global LOGFILTER
	for opt, arg in opts:
	  if opt in ("-h", "--help"):
	    usage()   
	    sys.exit(0)
	  elif opt in ("-f", "--filter"):
	    regexfilter = arg
	  elif opt in ("-f", "--exclude"):
	    excludes.append(arg)
	  elif opt in ("-k", "--keep"):
	    keep_jobs.append(arg)
	  elif opt in ("-s", "--create-symlinks"):
	    symlinks = True
	  elif opt in ("--zfs-binary", ):
	    global ZFS_BINARY
	    ZFS_BINARY = arg
	  elif opt in ("-v", "--verbose"):
	    LOGFILTER=1
	    verbose = True
	  elif opt in ("-v", "--debug"):
	    LOGFILTER=2
	    debug = True
	  elif opt in ("--run", ):
	    doit = True
	try:
	  re.compile(regexfilter)
	except Exception, e:
	  log("Invalid or missing filter (%s)." % str(e))
	  usage()
	  sys.exit(2)
	try:
	  for line in excludes:
	    re.compile(line)
	except Exception, e:
	  log("Invalid exclude filter (%s)." % str(e) )
	  usage()
	  sys.exit(2)
	if not lockFile('/var/run/zfs_snapshot.lock'):
	  log("Lockfile defined and locked, exiting.", "DEBUG")
	  sys.exit(0)
	keep_jobs = parse_keep_jobs(keep_jobs)
	(request_snap, request_link) = cleanup_snapshots(get_snaplist(regexfilter, excludes), keep_jobs, doit=doit)	
	create_snapshots(request_snap, doit=doit)
	if symlinks: update_symlinks(request_snap + request_link, doit=doit)

if __name__ == "__main__":
	main(sys.argv[1:])



