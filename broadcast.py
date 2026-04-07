#!/usr/bin/env python3

from maelstrom import Node, Request, Body
from collections import defaultdict
import asyncio

STAR_TOPOLOGY = {
  "n0": [
    "n1",
    "n2",
    "n3",
    "n4",
    "n5",
    "n6",
    "n7",
    "n8",
    "n9",
    "n10",
    "n11",
    "n12",
    "n13",
    "n14",
    "n15",
    "n16",
    "n17",
    "n18",
    "n19",
    "n20",
    "n21",
    "n22",
    "n23",
    "n24",
  ],
  "n1": ["n0"],
  "n2": ["n0"],
  "n3": ["n0"],
  "n4": ["n0"],
  "n5": ["n0"],
  "n6": ["n0"],
  "n7": ["n0"],
  "n8": ["n0"],
  "n9": ["n0"],
  "n10": ["n0"],
  "n11": ["n0"],
  "n12": ["n0"],
  "n13": ["n0"],
  "n14": ["n0"],
  "n15": ["n0"],
  "n16": ["n0"],
  "n17": ["n0"],
  "n18": ["n0"],
  "n19": ["n0"],
  "n20": ["n0"],
  "n21": ["n0"],
  "n22": ["n0"],
  "n23": ["n0"],
  "n24": ["n0"],
}

class NodeState:
  def __init__(self, node):
    self.node = node
    self.network_topology = {}
    self.values = {}
    self.neighbor_versions = defaultdict(lambda: 0)
    self.current_version = 0
    self.lock = asyncio.Lock()

  def updateTopology(self, topology):
    self.network_topology = topology
  
  def getNeighbors(self):
    return self.network_topology[self.node.node_id]

  async def getNeighborVersion(self, neighborId):
    async with self.lock:
      return self.neighbor_versions[neighborId]

  async def getCurrentVersion(self):
    async with self.lock:
      return self.current_version


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
      return self.current_version, did_add

node = Node()
state = NodeState(node)

@node.handler
async def broadcast(req: Request) -> Body:
  incoming = req.body["message"]

  _, _ = await state.addValues([incoming])

  return {
    "type": "broadcast_ok"
  }

@node.handler
async def broadcast_multiple(req: Request) -> Body:
  messages = req.body["messages"]

  _, _ = await state.addValues(messages)

  return {"type": "broadcast_ok"}


@node.handler
async def read(_: Request) -> Body:
  return {
    "type": "read_ok",
    "messages": (await state.getValues())[0],
  }

@node.handler
async def topology(_: Request) -> Body:
  state.updateTopology(STAR_TOPOLOGY)
  return {
    "type": "topology_ok",
  }

async def updateNeighbor(neighborId):
  # check version of neighbor
  version = await state.getNeighborVersion(neighborId) + 1

  # get all values that need to be sent to neighbor based on its version
  values, sent_version = await state.getValues(version)

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

  await state.updateNeighborVersion(neighborId, sent_version)


async def _updateNeighbors():
  while True:
    await asyncio.sleep(0.15)
    neighbors = state.getNeighbors()
    # parallelize call
    for neighbor in neighbors:
      asyncio.create_task(updateNeighbor(neighbor))


def updateNeighbors():
  asyncio.get_running_loop().create_task(_updateNeighbors())


node.run(updateNeighbors)
