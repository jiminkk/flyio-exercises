#!/usr/bin/env python3

import math
from maelstrom import Node, Request, Body
from collections import defaultdict
import asyncio

class NodeState:
  def __init__(self, node):
    self.node = node
    self.network_topology = {}
    self.values = {}
    self.neighbor_versions = defaultdict(lambda: 0)
    self.current_version = 0
    self.lock = asyncio.Lock()
  
  def getNeighbors(self):
    return self.network_topology.get(self.node.node_id, [])

  async def getNeighborVersion(self, neighborId):
    async with self.lock:
      return self.neighbor_versions[neighborId]

  async def updateNeighborVersion(self, neighborId, version):
    async with self.lock:
      self.neighbor_versions[neighborId] = version

  async def getValues(self, minVersion=0):
    result = []

    async with self.lock:
      for value, version in self.values.items():
        if version >= minVersion:
          result.append(value)
      return result, self.current_version

  async def hasValue(self, val):
    async with self.lock:
      return val in self.values

  async def addValues(self, items):
    async with self.lock:
      next_version = self.current_version + 1
      did_add = False
      for item in items:
        if item not in self.values:
          self.values[item] = next_version
          did_add = True
      if did_add:
        self.current_version = next_version
      return did_add

node = Node()
state = NodeState(node)

@node.handler
async def broadcast(req: Request) -> Body:
  incoming = req.body["message"]

  await state.addValues([incoming])

  return {
    "type": "broadcast_ok"
  }

@node.handler
async def broadcast_multiple(req: Request) -> Body:
  messages = req.body["messages"]

  await state.addValues(messages)

  return {
    "type": "broadcast_multiple_ok"
  }

@node.handler
async def read(req: Request) -> Body:
  values, _ = await state.getValues()
  return {
    "type": "read_ok",
    "messages": values,
  }

@node.handler
async def topology(req: Request) -> Body:
  allIds = sorted(node.node_ids, key=lambda s: int(s[1:]))
  nodeLen = len(allIds)

  # implement lieutenant topology
  lieutenantsLen = max(2, round(math.sqrt(nodeLen)))

  # pick the same first nodes in sorted list of nodes so all node share the same lieutenants
  lieutenants = allIds[:lieutenantsLen]
  leaves = allIds[lieutenantsLen:]

  lieutenantTopology = {key: [] for key in lieutenants}
  leafTopology = {key: [] for key in leaves}

  # for each lieutenant, we will connect it to rest of the lieutenants + leaves to connect it to
  for i, l in enumerate(lieutenants):
    for other in lieutenants:
      if other != l:
        lieutenantTopology[l].append(other)
    for j, leaf in enumerate(leaves):
      if j % lieutenantsLen == i:
        lieutenantTopology[l].append(leaf)

  # for each leaf, connect it to the lieutenants it belongs to
  for i, leaf in enumerate(leaves):
    leafTopology[leaf] = [lieutenants[i % lieutenantsLen]]

  state.network_topology = lieutenantTopology | leafTopology
  return {
    "type": "topology_ok",
  }

async def updateNeighbor(neighborId):
  # check version of neighbor
  version = await state.getNeighborVersion(neighborId) + 1

  # get all values that need to be sent to neighbor based on its version
  values, snapshot_version = await state.getValues(version)
  if not values:
    return

  # send the values in multi-broadcast rpc
  request = Request(src=node.node_id, dest=neighborId, body={
    "type": "broadcast_multiple",
    "messages": values,
  })

  # split into chunks for >30
  response = await node.rpc(neighborId, request.body)
  if response["type"] == "error":
    return

  await state.updateNeighborVersion(neighborId, snapshot_version)


async def _updateNeighbor():
  while True:
    await asyncio.sleep(0.4)
    neighbors = state.getNeighbors()
    # parallelize call
    for neighbor in neighbors:
      asyncio.create_task(updateNeighbor(neighbor))

def updateNeighbors():
  asyncio.get_running_loop().create_task(_updateNeighbor())

node.run(updateNeighbors)
