from dataclasses import dataclass

from .errors import ConfigError, SafetyError


@dataclass(frozen=True)
class Standby:
    node: str
    slot: str


@dataclass(frozen=True)
class InstanceGroup:
    """Resolved instance group.

    A direct logical endpoint becomes an implicit single-node group here.  The
    YAML stays small while the rest of the program consumes one stable model.
    """

    key: str
    name: str
    primary: str
    standbys: tuple
    explicit: bool

    @property
    def nodes(self):
        return (self.primary,) + tuple(item.node for item in self.standbys)


class Topology:
    def __init__(self, config):
        self.config = config

    def dependencies(self, name):
        cluster = self.config.cluster(name)
        if cluster["type"] != "logical":
            return ()
        return tuple(
            target for target in (cluster["publisher"], cluster["subscriber"])
            if target in self.config.clusters
        )

    def closure(self, target):
        if target not in self.config.clusters:
            raise ConfigError("未知集群: %s" % target)
        result = []
        visiting = set()
        visited = set()

        def visit(name):
            if name in visiting:
                raise ConfigError("集群依赖存在循环: %s" % name)
            if name in visited:
                return
            visiting.add(name)
            for dependency in self.dependencies(name):
                visit(dependency)
            visiting.remove(name)
            visited.add(name)
            result.append(name)

        visit(target)
        return result

    def group(self, name):
        cluster = self.config.cluster(name)
        if cluster["type"] != "streaming":
            raise ConfigError("%s 不是 streaming 集群" % name)
        standbys = tuple(
            Standby(item["node"], self.config.physical_slot(name, item))
            for item in cluster.get("standbys") or []
        )
        return InstanceGroup("cluster:%s" % name, name, cluster["primary"], standbys, True)

    def logical_group(self, logical_name, field):
        target = self.config.cluster(logical_name)[field]
        if target in self.config.clusters:
            return self.group(target)
        return InstanceGroup(
            "node:%s" % target,
            field,
            target,
            (),
            False,
        )

    def groups(self, target):
        groups = []
        seen = set()
        for name in self.closure(target):
            cluster = self.config.cluster(name)
            if cluster["type"] == "streaming":
                candidates = (self.group(name),)
            elif cluster["type"] == "logical":
                candidates = (
                    self.logical_group(name, "publisher"),
                    self.logical_group(name, "subscriber"),
                )
            else:
                candidates = ()
            for group in candidates:
                if group.key not in seen:
                    seen.add(group.key)
                    groups.append(group)
        return groups

    def nodes(self, target):
        result = []
        seen = set()
        for group in self.groups(target):
            for node in group.nodes:
                if node not in seen:
                    seen.add(node)
                    result.append(node)
        return result

    def logical_clusters(self, target):
        return [
            name for name in self.closure(target)
            if self.config.cluster(name)["type"] == "logical"
        ]

    def action_order(self, target):
        result = []
        seen = set()
        for name in self.closure(target):
            cluster = self.config.cluster(name)
            if cluster["type"] == "logical":
                for field in ("publisher", "subscriber"):
                    endpoint = cluster[field]
                    if endpoint in self.config.nodes and endpoint not in seen:
                        seen.add(endpoint)
                        result.append(endpoint)
            if name not in seen:
                seen.add(name)
                result.append(name)
        return result

    def edges(self, target):
        edges = []
        seen_nodes = set()
        for name in self.closure(target):
            cluster = self.config.cluster(name)
            if cluster["type"] == "streaming":
                group = self.group(name)
                for node in group.nodes:
                    edges.append((name, node))
                    seen_nodes.add(node)
            elif cluster["type"] == "logical":
                for field in ("publisher", "subscriber"):
                    endpoint = cluster[field]
                    edges.append((name, endpoint))
                    if endpoint in self.config.nodes:
                        seen_nodes.add(endpoint)
            elif cluster["type"] == "mmr":
                for node in cluster.get("nodes") or []:
                    edges.append((name, node))
                    seen_nodes.add(node)
        for node in sorted(seen_nodes):
            edges.append((node, self.config.node(node)["host"]))
        return edges

    def external_dependents(self, target):
        """Return clusters outside target closure that use resources it owns."""
        removal = set(self.closure(target))
        removal_nodes = set(self.nodes(target))
        blocked = set()
        for name, cluster in self.config.clusters.items():
            if name in removal:
                continue
            if any(dependency in removal for dependency in self.dependencies(name)):
                blocked.add(name)
                continue
            if cluster.get("type") == "logical":
                for field in ("publisher", "subscriber"):
                    endpoint = cluster[field]
                    if endpoint in self.config.nodes and endpoint in removal_nodes:
                        blocked.add(name)
        return sorted(blocked)

    def require_removable(self, target, action):
        blocked = self.external_dependents(target)
        if blocked:
            raise SafetyError(
                "%s 被其他集群依赖，不能直接%s；请先处理: %s" %
                (target, action, ", ".join(blocked))
            )
