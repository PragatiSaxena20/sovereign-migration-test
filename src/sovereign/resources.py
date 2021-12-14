import yaml
from sovereign.config_loader import load_python

with open("test/config/config.yaml") as f:
    config = yaml.safe_load(f)
sources = config["sources"]

template = config["templates"][1.18]["clusters"]
_, path = template.split("://")


async def clusters():
    resources = list()
    fn = await load_python(path)
    for source in sources:
        if source["config"].get("scope", "default") == "default":
            instances = [
                instance
                for instance in source["config"]["instances"]
                if instance.get("service_clusters") == ["X1"]
            ]
            resources.extend(fn.call(instances, None))  # type: ignore
    return resources
