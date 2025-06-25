import typer
from enum import Enum
from purr_petra_cli.proj import say_hello

app = typer.Typer()


class Asset(str, Enum):
    core = "core"
    dst = "dst"
    formation = "formation"
    ip = "ip"
    perforation = "perforation"
    production = "production"
    raster_log = "raster_log"
    survey = "survey"
    vector_log = "vector_log"
    well = "well"
    zone = "zone"


asset_members_str = ", ".join([member.value for member in Asset])


@app.command()
def collect(
    proj: str = typer.Option(..., help="Path to project directory, probably a UNC"),
    asset: Asset = typer.Option(
        Asset.well, help=f"The asset (choose from: {asset_members_str})"
    ),
    uwis: str = typer.Option(
        None,
        help="Full or partial UWI list. Separate UWIs with spaces or commas. "
        + "Use * or % as wildcards. This is a single arg, so wrap it in quotes."
        + '  Example: --uwis "05010001102000 350112* 350110*"  '
        + "omitting the --uwis arg selects everything--BEWARE!",
    ),
):
    # asset = asset.value # to get a real string from the Enum causes type error

    print("proj=", proj)
    print("asset=", asset.value)
    print("uwi_query", uwis)


if __name__ == "__main__":
    app()
