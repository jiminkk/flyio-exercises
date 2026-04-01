#!/usr/bin/env python3

from maelstrom import Node, Request, Body
from uuid import uuid4
import time

node = Node()

mask_64 = 0xffffffffffffffff
mask_63 = 0xffffffffffffff00
mask_8 = 0x00000000000000ff

@node.handler
async def generate(req: Request) -> Body:
  timestamp = time.time_ns()
  node_id = int(node.node_id[1:])

  msg_id = ((mask_64 & timestamp) & mask_63) | (node_id & mask_8)

  return {
    "type": "generate_ok",
    "id": msg_id,
    # "id": str(uuid4()),
  }

node.run()