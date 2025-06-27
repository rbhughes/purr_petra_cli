from typing import List
from purr_petra_cli.util import get_recipe
from purr_petra_cli.sql_helper import make_where_clause, chunk_ids, create_selectors
from purr_petra_cli.xformer import PURR_WHERE
from purr_petra_cli.dbisam import make_conn_params, db_exec


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


####################################################################


def select_assets(proj: str, asset: str, uwis_list: List[str] | None):
    recipe = get_recipe(asset)

    where = make_where_clause(uwis_list)

    id_sql = recipe["identifier"].replace(PURR_WHERE, where)

    # chunk_size = recipe["chunk_size"] if "chunk_size" in recipe else 1000
    chunk_size = 3

    conn = make_conn_params(proj)

    ids = fetch_id_list(conn, id_sql)

    if len(ids) == 0:
        return "Query returned zero hits"

    chunked_ids = chunk_ids(ids, chunk_size)

    # TODO: break out recipe args
    selectors = create_selectors(chunked_ids, recipe)

    print(".............................")
    print(where)
    print(".............................")
    print(id_sql)
    print(".............................")
    print(chunk_size)
    print(".............................")
    print(conn)
    print(".............................")
    print(ids)
    print(".............................")
    print(chunked_ids)
    print(".............................")
    for sql in selectors:
        print(sql)

    return "asdf"
