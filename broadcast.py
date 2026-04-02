#!/usr/bin/env python3


from maelstrom import Node, Request, Body

node = Node()
topology = {}
values = set()

@node.handler
async def broadcast(req: Request) -> Body:
  incoming = req.body["message"]

  if incoming in values:
    return {
      "type": "broadcast_ok"
    }

  values.add(incoming)
  neighbors = topology[node.node_id]
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
    "messages": list(values),
  }

@node.handler
async def topology(req: Request) -> Body:
  global topology
  topology = req.body["topology"]
  return {
    "type": "topology_ok",
  }

node.run()