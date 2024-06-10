from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger

from statickg.main import ETLPipelineRunner
from statickg.models.prelude import GitRepository

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def deploy_loop(
    cfg: Annotated[
        Path,
        typer.Argument(
            help="A path to a file containing the configuration of the pipeline",
            exists=True,
            dir_okay=False,
        ),
    ],
    workdir: Annotated[
        Path, typer.Argument(help="A directory for storing intermediate ETL results")
    ],
    datadir: Annotated[
        Path, typer.Argument(help="A directory containing the data Git repository")
    ],
    refresh: Annotated[
        float, typer.Option(help="Data refresh interval in seconds")
    ] = 1.0,
    loop: Annotated[
        bool, typer.Option("--loop/--no-loop", help="Continuously monitor for updates")
    ] = True,
    overwrite_config: Annotated[
        bool,
        typer.Option(
            "--overwrite-config/--no-overwrite-config",
            help="Overwrite the configuration file if it exists",
        ),
    ] = False,
):
    repo = GitRepository(datadir)
    kgbuilder = ETLPipelineRunner.from_config_file(cfg, workdir, repo, overwrite_config)

    # run a loop to continously deploy the pipeline.
    is_waiting = False

    if repo.fetch():
        kgbuilder()

    while loop:
        has_new_data = repo.fetch()
        if has_new_data:
            logger.info(
                "Found new changes in the data repository. Rerun the pipeline..."
            )
            kgbuilder()
            is_waiting = False

        if not is_waiting:
            logger.info("Wait for new changes...")
            is_waiting = True
        else:
            time.sleep(refresh)


if __name__ == "__main__":
    app()
