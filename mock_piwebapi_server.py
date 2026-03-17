#!/usr/bin/env python3
"""Mock AVEVA / OSIsoft PI Web API server."""

from __future__ import annotations

import argparse

from piwebapi.auth import build_users
from piwebapi.model import build_default_model
from piwebapi.server import make_server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PI Web API mock server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument(
        "--seed",
        default="piwebapi-mock-seed",
        help="Seed controlling deterministic value generation",
    )
    args = parser.parse_args()

    model = build_default_model(seed=args.seed)
    users = build_users()
    server = make_server(args.host, args.port, model, users)

    total_elements = len(model.elements_by_webid)
    total_attributes = len(model.attributes_by_webid)
    print(
        f"Mock PI Web API listening on http://{args.host}:{args.port}/piwebapi "
        f"(databases={len(model.databases_by_webid)}, elements={total_elements}, "
        f"attributes={total_attributes}, users={len(users)})"
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
