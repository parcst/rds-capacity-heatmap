#!/usr/bin/env python3
"""Dev entry point - run the server with auto-reload."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "rds_capacity_heatmap.app:app",
        host="127.0.0.1",
        port=8001,
        reload=True,
    )
