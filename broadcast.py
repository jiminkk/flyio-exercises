#!/usr/bin/env python3

from maelstrom import Node, Request, Body
import asyncio
import sys

class NodeState:
  def __init__(self, node):
    self.node = node
    self.network_topology = {}
    self.values = {}
    self.current_version = 0
    self.lock = asyncio.Lock()
  
  def getNeighbors(self):
    return self.network_topology[self.node.node_id]
  
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
      for item in items:
        if item not in self.values:
          self.values[item] = self.current_version
      self.current_version += 1

node = Node()
state = NodeState(node)

@node.handler
async def broadcast(req: Request) -> Body:
  incoming = req.body["message"]

  if await state.hasValue(incoming):
    return {
      "type": "broadcast_ok"
    }

  await state.addValues([incoming])

  neighbors = state.getNeighbors()
  for neighbor in neighbors:
    neighborReq = Request(src=node.node_id, dest=neighbor, body=req.body)
    node.spawn(node.send(neighborReq))

  return {
    "type": "broadcast_ok"
  }

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

async def _readFromNeighbor(neighbor):
  neighborReq = Request(src=node.node_id, dest=neighbor, body={
    "type": "read",
  })
  response = await node.rpc(neighbor, neighborReq.body)
  if response["type"] == "error":
    return

  await state.addValues(response["messages"])

async def _pollNeighbors():
  while True:
    await asyncio.sleep(1)
    neighbors = state.getNeighbors()
    # parallelize call
    for neighbor in neighbors:
      asyncio.create_task(_readFromNeighbor(neighbor))

def pollNeighbors():
  asyncio.get_running_loop().create_task(_pollNeighbors())

node.run(pollNeighbors)











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