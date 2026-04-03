#!/usr/bin/env python3

from maelstrom import Node, Request, Body
import asyncio
import sys

node = Node()
lock = asyncio.Lock()
# can't be named `topology`, the handler function apparently would shadow this variable
network_topology = {}
values = set()

@node.handler
async def broadcast(req: Request) -> Body:
  incoming = req.body["message"]

  if incoming in values:
    return {
      "type": "broadcast_ok"
    }

  async with lock:
    values.add(incoming)

  neighbors = network_topology[node.node_id]
  for neighbor in neighbors:
    neighborReq = Request(src=node.node_id, dest=neighbor, body=req.body)
    node.spawn(node.send(neighborReq))
    # node.spawn(_sendMessageWithRetry(neighbor, incoming))

  return {
    "type": "broadcast_ok"
  }

@node.handler
async def read(req: Request) -> Body:
  async with lock:
    return {
      "type": "read_ok",
      "messages": list(values),
    }

@node.handler
async def topology(req: Request) -> Body:
  global network_topology
  network_topology = req.body["topology"]
  return {
    "type": "topology_ok",
  }

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

async def _readFromNeighbor(neighbor):
  neighborReq = Request(src=node.node_id, dest=neighbor, body={
    "type": "read",
  })
  response = await node.rpc(neighbor, neighborReq.body)
  if response["type"] == "error":
    print("--- ERROR readFromNeighbor on " + neighbor, file=sys.stderr)
    return

  print("--- SUCCESS readFromNeighbor --- ", file=sys.stderr)
  async with lock:
    global values
    values |= set(response["messages"])

async def _pollNeighbors():
  while True:
    await asyncio.sleep(1)
    print("node.node_id " + node.node_id, file=sys.stderr)
    neighbors = network_topology[node.node_id]
    # parallelize call
    for neighbor in neighbors:
      asyncio.create_task(_readFromNeighbor(neighbor))

def pollNeighbors():
  print("polling neighbors", file=sys.stderr)
  # loop = asyncio.get_running_loop()
  # loop.run_in_executor(None, _pollNeighbors)
  asyncio.get_running_loop().create_task(_pollNeighbors())

node.run(pollNeighbors)
