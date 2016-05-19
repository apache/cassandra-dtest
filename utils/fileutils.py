import os
from dtest import debug  # Depending on dtest is not good long-term.


def size_of_files_in_dir(dir_name, verbose=True):
    """
    Return the size of all files found in a non-recursive ls of the argument.
    Based on http://stackoverflow.com/a/1392549
    """
    files = [os.path.join(dir_name, f) for f in os.listdir(dir_name)]
    if verbose:
        debug('getting sizes of these files: {}'.format(files))
    return sum(os.path.getsize(f) for f in files)
