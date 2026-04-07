#!/usr/bin/env python3

from maelstrom import Node, Request, Body
from collections import defaultdict
import asyncio
import sys

class NodeState:
  def __init__(self, node):
    self.node = node
    self.network_topology = {}
    self.values = {}
    self.neighbor_versions = defaultdict(lambda: 0)
    self.current_version = 0
    self.lock = asyncio.Lock()
  
  def getNeighbors(self):
    return self.network_topology[self.node.node_id]

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
      return result

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

  did_add = await state.addValues([incoming])
  if not did_add:
    return {
      "type": "broadcast_ok"
    }

  return {
    "type": "broadcast_ok"
  }

@node.handler
async def broadcast_multiple(req: Request) -> Body:
  messages = req.body["messages"]

  did_add = await state.addValues(messages)
  if not did_add:
    return {
      "type": "broadcast_multiple_ok"
    }
  
  neighbors = state.getNeighbors()
  for neighbor in neighbors:
    node.spawn(updateNeighbor(neighbor))


@node.handler
async def read(req: Request) -> Body:
  return {
    "type": "read_ok",
    "messages": await state.getValues(),
  }

@node.handler
async def topology(req: Request) -> Body:
  state.network_topology = req.body["topology"]
  return {
    "type": "topology_ok",
  }

async def updateNeighbor(neighborId):
  # check version of neighbor
  version = await state.getNeighborVersion(neighborId) + 1

  # get all values that need to be sent to neighbor based on its version
  values = await state.getValues(version)

  # send the values in multi-broadcast rpc
  request = Request(src=node.node_id, dest=neighborId, body={
    "type": "broadcast_multiple",
    "messages": values,
  })

  # split into chunks for >30
  response = await node.rpc(neighborId, request.body)
  if response["type"] == "error":
    return

  await state.updateNeighborVersion(neighborId, state.current_version)


async def _updateNeighbor():
  while True:
    await asyncio.sleep(1)
    neighbors = state.getNeighbors()
    # parallelize call
    for neighbor in neighbors:
      asyncio.create_task(updateNeighbor(neighbor))

def updateNeighbors():
  asyncio.get_running_loop().create_task(_updateNeighbor())

node.run(updateNeighbors)










# sendMessageWithRetry
# async def _sendMessageWithRetry(neighbor, message):
#   neighborReq = Request(src=node.node_id, dest=neighbor, body={
#     "type": "broadcast",
#     "message": message,
#   })

#   while True:
#     response = await node.rpc(neighbor, neighborReq.body)
#     if response["type"] == "error":
#       print("--- ERROR sendMessageWithRetry on " + neighbor, file=sys.stderr)
#       # await asyncio.sleep(0.5)
#       continue

#     print("--- SUCCESS sendMessageWithRetry --- ", file=sys.stderr)
#     return