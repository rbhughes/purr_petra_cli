from typing import Optional, List
import importlib
import hashlib
import time
from typing import Dict, Any

from pathlib import Path


def parse_uwis(uwis: Optional[str]) -> List[str] | None:
    """Parse POSTed uwi string into a suitable SQLAnywhere SIMILAR TO clause.
    Split by commas or spaces, replace '*' with '%', joined to '|'

    Example:
        0505* pilot %0001 -> '0505%|pilot|%0001'

    Args:
        uwis (Optional[str]): incoming request string

    Returns:
        List[str]: A list of UWI strings if present
    """
    if uwis is None:
        return uwis

    try:
        split = [
            item
            for item in uwis.strip().replace(",", " ").replace('"', "").split()
            if item
        ]
        parsed = [item.replace("*", "%").replace("'", "''") for item in split]
        # logger.debug(f"parse_uwi returns: {parsed}")
        return parsed
    except AttributeError as ae:
        print(ae)
        # logger.error(f"'uwis' must be a string, not {type(uwis)}")
        return []
    except Exception as e:  # pylint: disable=broad-except
        # logger.error(f"Unexpected error occurred: {str(e)}")
        print(e)
        return []


def get_recipe(asset: str) -> Dict[str, Any]:
    module_path = f"purr_petra_cli.recipes.{asset}"
    try:
        recipe_module = importlib.import_module(module_path)
        return getattr(recipe_module, "recipe")

    except ModuleNotFoundError as mnfe:
        print(f"Error: failed to find recipe at: {module_path}")
        raise mnfe

    except AttributeError as ae:
        print(f"Error: No recipe in module: '{module_path}'")
        raise ae


def timestamp_filename(proj: str, asset: str):
    fp = Path(proj)
    prefix = fp.name.upper()[:3]
    if len(fp.name) < 3:
        prefix = prefix.ljust(3, "_")
    suffix = hashlib.md5(str(fp).lower().encode()).hexdigest()[:6]
    repo_id = f"{prefix}_{suffix}"

    return f"{repo_id}_{int(time.time_ns())}_{asset}".lower()


def ensure_dir(dir: str) -> str:
    Path(dir).mkdir(parents=True, exist_ok=True)
    return str(dir)
