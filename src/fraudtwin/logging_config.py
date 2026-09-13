import logging


def configure_logging(verbose: bool = False) -> None:
    """Configure concise console logging for CLI commands."""

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
