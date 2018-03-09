# ZFS Automatic Snapshots

Create snapshots of ZFS filesystems automatically. Define how many snapshots to keep within a range of time and create or delete the snapshots do satisfy the given requirements. Create symlinks to expose the snapshots to windows as [shadow copies via samba](https://www.samba.org/samba/docs/current/man-html/vfs_shadow_copy2.8.html).

### Warning

No batteries included ... this is just the code i use. It's not very actively maintained but also not neglected. It's just here in case it's useful for somebody.

# Usage

    ./zas.py list <filesystem>... [--keep=<time>...] [options]
    ./zas.py manage <filesystem>... --keep=<time>... [--run] [options]

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

## Examples

Keep snapshots of filesystem tank/backup, that are not older than 1 through 6 hours, 1 through 7 days and each quarter of a year:

    $ ./zas.py manage tank/backup --keep=1H*6,1d*7,1y/4 -r        `
