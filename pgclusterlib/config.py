import os
import re
from pathlib import Path

import yaml

from .errors import ConfigError


ENV = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}$")


def _expand(value):
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if not isinstance(value, str):
        return value
    match = ENV.match(value)
    if not match:
        return os.path.expanduser(value)
    name, default = match.groups()
    actual = os.environ.get(name)
    if actual:
        return os.path.expanduser(actual)
    if default is not None:
        return os.path.expanduser(default)
    raise ConfigError("环境变量未设置: %s" % name)


def load(path):
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise ConfigError("无法读取配置 %s: %s" % (path, exc))
    except yaml.YAMLError as exc:
        raise ConfigError("YAML 解析失败: %s" % exc)
    from .config_model import ConfigModel
    return ConfigModel(path, raw)


from .config_model import ConfigModel

Config = ConfigModel
