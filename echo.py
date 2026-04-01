#!/usr/bin/env python3

import sys
from maelstrom import Node, Request, Body

node = Node()

@node.handler
async def echo(req: Request) -> Body:
  print("node id", node.node_id, file=sys.stderr)

  return {
    "type": "echo_ok",
    "echo": req.body["echo"],
    "in_reply_to": req.body["msg_id"],
  }


node.run()