from subprocess import Popen, PIPE


def command(cmd, check=False, **params):
    p = Popen(
        [cmd],
        shell=True,
        stdout=PIPE,
        stderr=PIPE,
        **params
    )

    if check is True:
        p.wait()
        if p.returncode != 0:
            raise Exception('Command fail', {
                'error': p.stderr.read()
            })

    return p
