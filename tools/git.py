import subprocess
import logging

logger = logging.getLogger(__name__)


def cassandra_git_branch(cassandra_dir):
    '''Get the name of the git branch at CASSANDRA_DIR.
    '''
    try:
        p = subprocess.Popen(['git', 'branch'], cwd=cassandra_dir,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:  # e.g. if git isn't available, just give up and return None
        logger.debug('shelling out to git failed: {}'.format(e))
        return

    out, err = p.communicate()
    # fail if git failed
    if p.returncode != 0:
        raise RuntimeError('Git printed error: {err}'.format(err=err.decode("utf-8")))
    [current_branch_line] = [line for line in out.decode("utf-8").splitlines() if line.startswith('*')]
    return current_branch_line[1:].strip()
