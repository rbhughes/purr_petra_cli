# purr_petra_cli

| <img src="./docs/purrio.png" alt="purrio logo" height="100"/> | <img src="./docs/snp.png" alt="purrio logo" width="200"/>| **PETRA** |
|:--:|:--:|:--:|


Use **purr_petra_cli** to query Petra projects* from the command line and generate well-centric json. This is a stripped-down implementation of most export functionality of [purr_petra](https://github.com/rbhughes/purr_petra).


```
- core
- dst
- formation
- ip
- perforation
- production
- raster_log
- survey
- vector_log
- well
- zone
```


* _Each [Petra](https://www.spglobal.com/commodityinsights/en/ci/products/petra-geological-analysis.html) "project" is a semi-structured collection
  of E&P assets that interoperate with its own
  [DBISAM](https://www.elevatesoft.com/products?category=dbisam)
  database. From an IT perspective, Petra is a distributed collection of
  user-managed databases "on the network" containing millions of assets._


## WHO IS THIS FOR?

This was developed as part of a Databricks geospatial catalog pipeline. It is useful for quickly exporting data from Petra, particularly if you don't have a license or want to avoid Petra's cumbersome ASCII export.


## LIMITATIONS:

If you are trying to egress lots of data from a large Petra project, this is NOT the tool for you. It relies solely on ODBC via Python. You are better off exporting/parsing a .PPF file.

**purr_petra_cli** should be fine for smaller projects ~ roughly those with < 100000 wells. Use the `--uwis` filter to limit state/county.


## QUICKSTART:


1. Install from pypi:  

`pip install purr_petra_cli`


2. Register the DBISAM dll:  

launch console "as Administrator"  

`purr-petra-cli setup`  

3. Try it out:  

`Usage: purr-petra-cli [OPTIONS] COMMAND [ARGS]...`

`purr-petra-cli --help`

`purr-petra-cli collect --help`

```
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --proj              TEXT                                           Path to project directory, It's probably a UNC │
│                                                                       path                                           │
│                                                                       [default: None]                                │
│                                                                       [required]                                     │
│    --asset             [core|dst|formation|ip|perforation|production  The asset (choose from: core, dst, formation,  │
│                        |raster_log|survey|vector_log|well|zone]       ip, perforation, production, raster_log,       │
│                                                                       survey, vector_log, well, zone)                │
│                                                                       [default: well]                                │
│    --uwis              TEXT                                           Full or partial UWI list. Separate UWIs with   │
│                                                                       spaces or commas. Use * or % as wildcards.     │
│                                                                       This is a single arg, so wrap it in quotes.    │
│                                                                       Example: --uwis "05010001102000 350112*        │
│                                                                       350110*"  omitting the --uwis arg selects      │
│                                                                       everything--BEWARE!                            │
│                                                                       [default: None]                                │
│    --output-dir        TEXT                                           Define an output directory store exported json │
│                                                                       data. Enclose paths with spaces in             │
│                                                                       quotes.(Defaults to System TEMP).              │
│                                                                       [default: C:\Users\bryan\AppData\Local\Temp]   │
│    --help                                                             Show this message and exit.                    │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```


`purr-petra-cli collect --proj c:\projects\MYPROJ --output-dir c:\outdir --uwis "05003*" --asset well`

`purr-petra-cli collect --proj "c:\projects\SPACE PROJ" --output-dir "c:\path with spaces\out" --uwis "05003*" --asset raster_log`