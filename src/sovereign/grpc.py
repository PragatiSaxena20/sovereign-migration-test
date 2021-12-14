import asyncio
from typing import Optional, List

from betterproto.lib.google.protobuf import Any
from envoy_data_plane.envoy.api.v2 import (
    ClusterDiscoveryServiceBase,
    DiscoveryResponse,
)
from envoy_data_plane.envoy.api.v2 import core
from envoy_data_plane.google import rpc as _google_rpc
from grpclib.server import Server

from sovereign.resources import clusters



class CDS(ClusterDiscoveryServiceBase):
    async def fetch_clusters(
            self,
            version_info: str,
            node: "core.Node",
            resource_names: Optional[List[str]],
            type_url: str,
            response_nonce: str,
            error_detail: "_google_rpc.Status",
    ) -> "DiscoveryResponse":
        print(version_info, node, resource_names, type_url, response_nonce, error_detail)
        # node_ = node.to_dict()
        # node_['metadata'] = node_['metadata']['fields']
        # print('_-----_')
        # print(node_)
        resp = DiscoveryResponse()
        resources: List[Any] = list()
        for res in await clusters():
            resources.append(Any(
                type_url=type_url,
                value=res.SerializeToString()
            ))
        return DiscoveryResponse(
            version_info=resp.version_info,
            resources=resources
        )


async def start_server():
    HOST = "127.0.0.1"
    PORT = 50051
    server = Server([CDS()])
    print('Starting GRPC server')
    await server.start(HOST, PORT)
    await server.wait_closed()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_server())
    loop.close()

