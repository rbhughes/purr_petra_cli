from typing import List, Dict, Any
from pathlib import Path
from purr_petra_cli.sql_helper import get_column_info
from purr_petra_cli.xformer import formatters, transform_dataframe_to_json
from purr_petra_cli.asset_config import AssetConfig
import json
import pyodbc
import pandas as pd
import numpy as np


def compose_and_write_docs(
    cfg: AssetConfig, select: str, out_file: str
) -> tuple[str, int]:
    all_columns = set()

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("[")  # Start of JSON array
        docs_written = 0

        with pyodbc.connect(**cfg.conn) as con:
            cursor = con.cursor()
            cursor.execute(select)
            column_names, column_types = get_column_info(cursor)
            df = pd.DataFrame(
                [tuple(row) for row in cursor.fetchall()], columns=column_names
            )

            all_columns.update(df.columns)

            for col in df.columns:
                col_type = str(df.dtypes[col])
                xform = cfg.xforms.get(col, col_type)
                formatter = formatters.get(xform, lambda x: x)
                df[col] = df[col].apply(formatter)

            df = df.replace({np.nan: None})

            if cfg.post_processor:
                df = cfg.post_processor(df)

            json_data: List[Dict[str, Any]] = transform_dataframe_to_json(
                df, cfg.prefixes
            )

            for json_obj in json_data:
                json_obj["proj"] = cfg.proj
                json_str = json.dumps(json_obj, default=str)
                f.write(json_str + ",")
                docs_written += 1

        if docs_written > 0:
            f.seek(f.tell() - 1, 0)  # Remove the last comma
        f.write("]")  # End JSON array

    return out_file, docs_written


def select_assets(proj: str, asset: str, uwis_list: List[str] | None, output_dir: str):
    cfg = AssetConfig(asset=asset, proj=proj, uwis_list=uwis_list)

    out_files: List[str] = []

    for select in cfg.selectors:
        out_file = str(Path(output_dir, cfg.out_file()).with_suffix(".json"))
        out_files.append(out_file)
        result = compose_and_write_docs(cfg, select, out_file)

        print(result)
        print("................")

    # result = compose_and_write_docs(cfg)

    # print(result)
