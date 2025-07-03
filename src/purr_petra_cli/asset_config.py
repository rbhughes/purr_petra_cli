from dataclasses import dataclass
from typing import List, Optional
from purr_petra_cli.util import get_recipe, timestamp_filename
from purr_petra_cli.sql_helper import make_where_clause, create_selectors, chunk_ids
from purr_petra_cli.dbisam import make_conn_params, db_exec
from purr_petra_cli.post_process import post_process
from purr_petra_cli.xformer import PURR_WHERE


def fetch_id_list(conn, id_sql):
    """
    Executes and asset recipe's identifier SQL and returns ids.
    :return: Results will be either be a single "keylist"
    [{keylist: ["1-62", "1-82", "2-83", "2-84"]}]
    or a list of key ids
    [{key: "1-62"}, {key: "1-82"}, {key: "2-83"}, {key: "2-84"}]
    Force int() or str(); the typical case is a list of int
    """

    def int_or_string(obj):
        try:
            return int(obj)
        except ValueError:
            return f"'{str(obj).strip()}'"

    res = db_exec(conn, id_sql)

    if isinstance(res, Exception):
        print(res)
        return []

    ids = []

    if not res:
        print("no ids found")
    elif "keylist" in res[0] and res[0]["keylist"] is not None:
        ids = res[0]["keylist"].split(",")
    elif "key" in res[0] and res[0]["key"] is not None:
        ids = [k["key"] for k in res]
    else:
        print("key or keylist missing; cannot make id list")

    return [int_or_string(i) for i in ids]


@dataclass
class AssetConfig:
    asset: str
    proj: str
    uwis_list: Optional[List[str]]

    def out_file(self):
        return timestamp_filename(self.proj, self.asset)

    @property
    def recipe(self):
        return get_recipe(self.asset)

    @property
    def conn(self):
        return make_conn_params(self.proj)

    @property
    def xforms(self):
        return self.recipe.get("xforms", {})

    @property
    def prefixes(self):
        return self.recipe.get("prefixes", {})

    @property
    def selectors(self):
        where = make_where_clause(self.uwis_list)

        id_sql = self.recipe["identifier"].replace(PURR_WHERE, where)
        # chunk_size = self.recipe["chunk_size"] if "chunk_size" in self.recipe else 1000 chunk_size = 3
        chunk_size = 6

        ids = fetch_id_list(self.conn, id_sql)

        if len(ids) == 0:
            return "Query returned zero hits"

        chunked_ids = chunk_ids(ids, chunk_size)

        selectors = create_selectors(
            chunked_ids=chunked_ids,
            identifier_keys=self.recipe["identifier_keys"],
            selector=self.recipe["selector"],
        )

        return selectors

    @property
    def post_processor(self):
        if proc_name := self.recipe.get("post_process"):
            return post_process[proc_name]
