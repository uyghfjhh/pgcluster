import os
import shlex
import socket
import subprocess
from pathlib import Path

from .errors import OperationError


# Do not resolve the local FQDN here.  On a host without reverse DNS that
# lookup can delay every command invocation by several seconds.
LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost", socket.gethostname()}


def shell_join(args):
    join = getattr(shlex, "join", None)
    if join:
        return join(args)
    return " ".join(shlex.quote(str(item)) for item in args)


class OperationLog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("w", encoding="utf-8")
        self.step = 0

    def record(self, host, cwd, command, stdin, stdout, stderr, code):
        self.step += 1
        stdin_section = "\nSTDIN:\n%s\n" % stdin if stdin is not None else ""
        self.stream.write(
            "STEP %d\nHOST: %s\nCWD: %s\n\nCOMMAND:\n%s\n%s\nSTDOUT:\n%s\n\nSTDERR:\n%s\n\nEXIT_CODE: %s\n\n" %
            (self.step, host, cwd, command, stdin_section, stdout, stderr, code)
        )
        self.stream.flush()

    def close(self):
        self.stream.close()


class Executor:
    """Run commands and filesystem operations on local or SSH hosts."""

    def __init__(self, operation_log=None):
        self.log = operation_log

    @staticmethod
    def is_local(address):
        return address in LOCAL_HOSTS

    def run(self, args, host="local", cwd=None, check=True, stdin=None):
        args = [str(item) for item in args]
        display = shell_join(args)
        remote = host != "local" and not self.is_local(host)
        if remote:
            display = "ssh %s -- %s" % (shlex.quote(host), display)
            actual = ["ssh", host, "--", shell_join(args)]
            actual_cwd = None
        else:
            actual = args
            actual_cwd = cwd
        try:
            process = subprocess.run(
                actual,
                cwd=actual_cwd,
                input=stdin,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise OperationError("无法执行命令 %s: %s" % (display, exc))
        if self.log:
            self.log.record(
                host, cwd or os.getcwd(), display, stdin,
                process.stdout, process.stderr, process.returncode,
            )
        if check and process.returncode:
            raise OperationError(
                "命令失败（退出码 %s）: %s\n%s" %
                (process.returncode, display, process.stderr.strip())
            )
        return process

    def exists(self, host, path):
        return self.run(["test", "-e", path], host=host, check=False).returncode == 0

    def is_dir(self, host, path):
        return self.run(["test", "-d", path], host=host, check=False).returncode == 0

    def is_nonempty_dir(self, host, path):
        script = 'test -d "$1" && test -n "$(find "$1" -mindepth 1 -print -quit)"'
        return self.run(["sh", "-c", script, "sh", path], host=host, check=False).returncode == 0

    def read_text(self, host, path, check=True):
        return self.run(["cat", "--", path], host=host, check=check).stdout

    def realpath(self, host, path):
        return self.run(["readlink", "-f", "--", path], host=host).stdout.strip()

    def write_text(self, host, path, content, mode="0600"):
        script = (
            'set -eu; target=$1; mode=$2; tmp="${target}.pgcluster.tmp.$$"; '
            'trap \'rm -f -- "$tmp"\' EXIT; umask 077; cat > "$tmp"; '
            'chmod "$mode" "$tmp"; mv -f -- "$tmp" "$target"; trap - EXIT'
        )
        self.run(["sh", "-c", script, "sh", path, mode], host=host, stdin=content)

    def append_text(self, host, path, content):
        self.run(["sh", "-c", 'cat >> "$1"', "sh", path], host=host, stdin=content)

    def remove_tree(self, host, path):
        # The caller must validate the marker first.  Positional arguments keep
        # the exact path out of shell syntax and avoid glob expansion.
        script = 'set -eu; find "$1" -depth -mindepth 1 -delete; rmdir -- "$1"'
        self.run(["sh", "-c", script, "sh", path], host=host)
