import subprocess

from dtest import CASSANDRA_DIR, debug


def cassandra_git_branch(cdir=None):
    '''Get the name of the git branch at CASSANDRA_DIR.
    '''
    cdir = CASSANDRA_DIR if cdir is None else cdir
    try:
        p = subprocess.Popen(['git', 'branch'], cwd=cdir,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:  # e.g. if git isn't available, just give up and return None
        debug('shelling out to git failed: {}'.format(e))
        return

    out, err = p.communicate()
    # fail if git failed
    if p.returncode != 0:
        raise RuntimeError('Git printed error: {err}'.format(err=err))
    [current_branch_line] = [line for line in out.splitlines() if line.startswith('*')]
    return current_branch_line[1:].strip()
