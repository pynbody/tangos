import os
import subprocess
import sys


def _check_import_is_clean():
    os.environ["TANGOS_PROPERTY_MODULES"]=""
    import tangos
    assert 'pynbody' not in sys.modules
    assert 'yt' not in sys.modules

def test_import_is_clean():
    """Ensure that pynbody is not imported when tangos is imported"""

    # The test has to be carried out in a separate process, because this process is contaminated by the multiple
    # imports that other tests and/or pytest plugins may initiate.
    exit_status = subprocess.call([sys.executable, __file__])
    assert exit_status==0

if __name__=="__main__":
    _check_import_is_clean()
