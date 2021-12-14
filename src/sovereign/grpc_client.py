import asyncio
from envoy_data_plane.envoy.api.v2 import ClusterDiscoveryServiceStub, Cluster

from grpclib.client import Channel


async def main():
    channel = Channel(host="127.0.0.1", port=50051)
    service = ClusterDiscoveryServiceStub(channel)
    response = await service.fetch_clusters(version_info="0", resource_names=[])
    stuff = response.resources[0].value
    print(Cluster().FromString(stuff))
    channel.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
