"""Convenience method for dealing with DBISAM via ODBC"""

from pathlib import Path
from typing import Any, Dict, List
import pyodbc
# from purr_petra.core.logger import logger


DBISAM_DRIVER = "DBISAM 4 ODBC Driver"


def db_exec(conn: Dict, sql: str) -> List[Dict[str, Any]] | Exception:
    """Convenience method for using pyodbc and DBISAM with Petra

    The dreaded "DBISAM Engine Error # 11013. Access denied to table or file"
    error might happen with malware scans or slow networks or bad luck, but
    removing asyncio seems to have mitigated most occurrences.

    Args:
        conn (dict): DBISAM connection parameters.
        sql (str): A single SQL statement to execute on the database.

    Returns:
        List[Dict[str, Any]]: list of dicts representing rows from query result.

    Raises:
        - pyodbc.ProgrammingError: For cases where table(s) might not exist due
        to an unxepected schema.
        - If the databases is older than DBISAM v4 (i.e Petra from about 2017),
        the general Exception will be something like: "not the correct version"
    """

    try:
        # pylint: disable=c-extension-no-member
        with pyodbc.connect(**conn) as connection:
            # I suspect S&P does not modify this per locale, but you should
            # probably verify the dbisam encoding if dealing with non-US data.
            # DBISAM says Locale = "ANSI Standard"
            connection.setencoding("CP1252")

            with connection.cursor() as cursor:
                cursor.execute(sql)

                return [
                    dict(zip([col[0] for col in cursor.description], row))
                    for row in cursor.fetchall()
                ]

    except pyodbc.ProgrammingError as pe:
        # logger.error(pe)
        raise pe
    except Exception as ex:
        # logger.error(ex)
        raise ex


def make_conn_params(proj: str) -> dict:
    """Assemble Petra-centric connection parameters used by pyodbc

    Args:
        repo_path (str): Path to a Petra project base directory.

    Returns:
        dict: dictionary of DBISAM connection parameters.
    """
    params = {"driver": DBISAM_DRIVER, "catalogname": str(Path(proj) / "DB")}
    return params


def make_parms_conn_params(proj: str) -> dict:
    """Assemble Petra-centric connection parameters for PARMS used by pyodbc

    Args:
        repo_path (str): Path to a Petra project base directory.

    Returns:
        dict: dictionary of DBISAM connection parameters.
    """
    params = {"driver": DBISAM_DRIVER, "catalogname": str(Path(proj) / "PARMS")}
    return params
