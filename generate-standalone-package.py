import os
import platform
import shutil
import subprocess

from flintrock import __version__ as flintrock_version

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

if __name__ == '__main__':
    operating_system = platform.system()
    if operating_system.lower() == 'darwin':
        operating_system = 'OSX'
    machine_type = platform.machine()

    subprocess.run([
            'pyinstaller',
            '--noconfirm',
            '--clean',
            '--name', 'flintrock',
            '--additional-hooks-dir', '.',
            # This hidden import is for AsyncSSH / Cryptography.
            # It will become unnecessary with resolution of this issue:
            # https://github.com/pyinstaller/pyinstaller/issues/1425
            '--hidden-import', '_cffi_backend',
            'standalone.py'],
        check=True)

    shutil.make_archive(
        base_name=os.path.join(
            THIS_DIR, 'dist',
            'Flintrock-{v}-standalone-{os}-{m}'.format(
                v=flintrock_version,
                os=operating_system,
                m=machine_type)),
        format='zip',
        root_dir=os.path.join(THIS_DIR, 'dist', 'flintrock'))
