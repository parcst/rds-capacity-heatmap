"""Entry point for ``python -m rds_capacity_heatmap``."""

import uvicorn


def main() -> None:
    uvicorn.run(
        "rds_capacity_heatmap.app:app",
        host="127.0.0.1",
        port=8001,
        reload=True,
    )


if __name__ == "__main__":
    main()
