import fcntl
from contextlib import contextmanager

from .errors import SafetyError


@contextmanager
def configuration_lock(config):
    path = config.path.with_name(".%s.lock" % config.path.name)
    try:
        stream = path.open("a+", encoding="utf-8")
    except OSError as exc:
        raise SafetyError("无法创建操作锁 %s: %s" % (path, exc))
    try:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SafetyError("另一个 pgcluster 操作正在使用配置: %s" % config.path)
        yield
    finally:
        stream.close()
