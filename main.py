import argparse
import json
import os
from tabulate import tabulate
from extract import read_sample
from db import drop_databases
from transform import find_functional_dependencies
from load import setup_stage, load_stage


def load_config():
    """Load the dataset configuration from etl.stage.json"""
    with open("etl.stage.json", "r") as f:
        return json.load(f)


def build_dataset_entries(config):
    """Build a flat list of all dataset entries from the hierarchical config"""
    entries = []
    
    for dataset_group in config["datasets"]:
        # Each dataset_group has one source key (SIAF, RENAMU, RENTAS, etc.)
        for source_name, source_metadata in dataset_group.items():
            # Extract database and schema from source level
            source_database = source_metadata.get("database")
            source_schema = source_metadata.get("schema")
            
            # Iterate over tables for this source
            for config_item in source_metadata.get("tables", []):
                names = config_item.get("names", [])
                years = config_item.get("years", [])
                variants = config_item.get("variants", {})
                modules = config_item.get("modules", [])
                file_type = config_item.get("type", "parquet")
                subdir = config_item.get("subdir", "")
                filename_format = config_item.get("filename_format", None)
                
                # Handle RENAMU special case: years + modules in subdirectory
                if modules and years:
                    for year in years:
                        for module in modules:
                            for name in names:
                                # Use custom format if provided, otherwise generate
                                if filename_format:
                                    filename = filename_format.format(year=year, module=module)
                                else:
                                    filename = f"{name}_{year}_modulo_{module}.{file_type}"
                                
                                # RENAMU files are in RENAMU_modules/ subdirectory
                                file_path = os.path.join(subdir, filename) if subdir else filename
                                entries.append({
                                    "display_name": f"{name} ({year}, Module {module})",
                                    "filename": filename,
                                    "file_path": file_path,
                                    "source": source_name,
                                    "base_name": name,
                                    "year": year,
                                    "module": module,
                                    "database": source_database,
                                    "tablename": f"{config_item['tablename']}_mod{module}",
                                    "schema": source_schema
                                })
                # Handle datasets with years and variants (e.g., INGRESO with DIARIO/MENSUAL)
                elif years and variants:
                    # Add regular years
                    for year in years:
                        for name in names:
                            filename = f"{year}-{name}.{file_type}"
                            entries.append({
                                "display_name": f"{name} ({year})",
                                "filename": filename,
                                "file_path": filename,
                                "source": source_name,
                                "base_name": name,
                                "year": year,
                                "database": source_database,
                                "tablename": f"{config_item['tablename']}",
                                "schema": source_schema
                            })
                    # Add variants
                    for year, variant_list in variants.items():
                        for variant in variant_list:
                            for name in names:
                                filename = f"{year}-{name}-{variant}.{file_type}"
                                entries.append({
                                    "display_name": f"{name} ({year}-{variant})",
                                    "filename": filename,
                                    "file_path": filename,
                                    "source": source_name,
                                    "base_name": name,
                                    "year": year,
                                    "variant": variant,
                                    "database": source_database,
                                    "tablename": f"{config_item['tablename']}_{variant.lower()}",
                                    "schema": source_schema
                                })
                # Handle datasets with years
                elif years:
                    for year in years:
                        for name in names:
                            base_name = f"{name}_{year}"
                            filename = f"{base_name}.{file_type}"
                            entries.append({
                                "display_name": f"{name} ({year})",
                                "filename": filename,
                                "file_path": filename,
                                "source": source_name,
                                "base_name": name,
                                "year": year,
                                "database": source_database,
                                "tablename": f"{config_item['tablename']}",
                                "schema": source_schema
                            })
                # Handle simple datasets
                else:
                    for name in names:
                        filename = f"{name}.{file_type}"
                        entries.append({
                            "display_name": name,
                            "filename": filename,
                            "file_path": filename,
                            "source": source_name,
                            "base_name": name,
                            "database": source_database,
                            "tablename": config_item["tablename"],
                            "schema": source_schema
                        })
    
    return entries


def find_dataset(entries, name):
    """Find a dataset entry by display name or filename"""
    name_lower = name.lower()
    
    for entry in entries:
        if (entry["filename"].lower() == name_lower or
            entry["filename"].lower() == f"{name_lower}.parquet" or
            entry["filename"].lower() == f"{name_lower}.csv" or
            entry["display_name"].lower() == name_lower or
            entry["base_name"].lower() == name_lower or
            name_lower in entry["display_name"].lower()):
            return entry
    
    return None


def process_single_dataset(entry, args):
    """Process a single dataset for staging"""
    print(f"\nProcessing: {entry['display_name']}")
    
    # file_path is relative without data/ prefix, load_stage/setup_stage add it
    file_path = entry["file_path"]
    
    if args.gen_schema:
        print("Creating schema...")
        setup_stage(
            file_path,
            args.database_name if args.database_name else entry["database"],
            args.table_name if args.table_name else entry["tablename"],
            args.schema if args.schema != "dbo" else entry["schema"],
            args.sample_size,
            args.optimize,
        )
    
    print("Loading data...")
    load_stage(
        file_path,
        args.database_name if args.database_name else entry["database"],
        args.table_name if args.table_name else entry["tablename"],
        args.schema if args.schema != "dbo" else entry["schema"]
    )
    
    return True


def gen_stage(args):
    config = load_config()
    entries = build_dataset_entries(config)
    
    # If --all flag is set, process all datasets
    if args.all:
        print(f"\nProcessing all {len(entries)} datasets from configuration...")
        print("=" * 70)
        successful = 0
        failed = 0
        missing = 0
        if args.drop:
            print("Deleting previous schema...")
            for entry in entries:
                drop_databases(args.database_name if args.database_name else entry["database"])
        for entry in entries:
            # Check for file existence with data/ prefix
            check_path = os.path.join("data", entry["file_path"])
            if not os.path.exists(check_path):
                print(f"SKIP (missing): {entry['display_name']} - {check_path}")
                missing += 1
                continue
            
            try:
                if process_single_dataset(entry, args):
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"ERROR processing {entry['display_name']}: {e}")
                failed += 1
                continue
        
        print("=" * 70)
        print(f"Results: Success: {successful}, Failed: {failed}, Missing: {missing}/{len(entries)}")
        return
    
    # Process single dataset
    if not args.name:
        print("Error: Either -n/--name or --all must be provided")
        print("\nAvailable datasets:")
        for e in entries:
            check_path = os.path.join("data", e["file_path"])
            status = "✓" if os.path.exists(check_path) else "✗"
            print(f"  {status} {e['display_name']}")
        return
    
    entry = find_dataset(entries, args.name)
    if not entry:
        print(f"Error: Dataset '{args.name}' not found in configuration")
        print("\nAvailable datasets:")
        for e in entries:
            check_path = os.path.join("data", e["file_path"])
            status = "✓" if os.path.exists(check_path) else "✗"
            print(f"  {status} {e['display_name']}")
        return
    
    check_path = os.path.join("data", entry["file_path"])
    if not os.path.exists(check_path):
        print(f"Error: File not found at {check_path}")
        return
    
    process_single_dataset(entry, args)


def gen_dmt(args): 
    fdeps = []
    if args.gen_deps:
        print("Loading...")
        sample = read_sample(args.dataset, 10000)
        print("Generating functional dependencies...")
        for col in sample.columns:
            deps = find_functional_dependencies(sample, col)
            fdeps.append(
                {
                    "Field": col,
                    "Possible func. deps": deps,
                }
            )
    else:
        with open(
            f"{args.depsfile}.txt" if args.depsfile else "depsfile.txt", "r"
        ) as depsfile:
            for _ in depsfile:
                line = _.rstrip()
                if line:
                    det, right = line.split(":")
                    deps = right.split(",")
                    fdeps.append({"Field": det, "Possible func. deps": deps})
            depsfile.close()

    print("Functional dependencies generated: ")
    print(tabulate(fdeps, headers="keys", tablefmt="grid", maxcolwidths=[None, 80]))
    correct = input("Is this correct? (y/N): ")
    if correct == "y":
        pass
    else:
        if args.gen_deps:
            save = input(
                "Do you want to save the generated dependencies to fdeps.txt to modify them later? (y/N) "
            )
            if save == "y":
                with open(
                    f"{args.depsfile}.txt" if args.depsfile else "depsfile.txt", "w"
                ) as depsfile:
                    for dict in fdeps:
                        depsfile.write(
                            f"{dict['Field']}:{''.join(f'{dep},' for dep in dict['Possible func. deps'])}\n"
                        )
                    depsfile.close()
                    print("Done.")

        print("Quitting...")


if __name__ == "__main__":
    config = load_config()
    entries = build_dataset_entries(config)
    
    parser = argparse.ArgumentParser(
        description="Tool to generate a SQL Server Datamart and dimensions from parquet/csv sources."
    )

    subparsers = parser.add_subparsers(dest="cmd")
    stage_parser = subparsers.add_parser("stage", help="Generation of STAGE database.")
    dim_parser = subparsers.add_parser("dmt", help="Generation of DIM and FACT tables")

    stage_parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="The name of the dataset from etl.stage.json. Required if --all is not used.",
        required=False,
    )
    stage_parser.add_argument(
        "--all",
        action="store_true",
        help="Load all datasets specified in etl.stage.json. Cannot be used with -n/--name.",
    )
    stage_parser.add_argument(
        "-d",
        "--database-name",
        type=str,
        help="The name of the database. Defaults to dataset name",
    )
    stage_parser.add_argument(
        "-s",
        "--schema",
        type=str,
        default="dbo",
        help="Name of the schema to use. Defaults to database default (dbo)."
    )
    stage_parser.add_argument(
        "-t",
        "--table-name",
        type=str,
        help="The name to use for the tables in the database. Defaults to dataset name",
    )
    stage_parser.add_argument(
        "-o",
        "--optimize",
        action="store_true",
        help="Try to optimize the database's disk space by fitting column types to the size of the data more closely. It may fail if a bigger data point was not captured on the sample."
    )
    stage_parser.add_argument(
        "--sample-size",
        type=int,
        default=1000000,
        help="Size of the sample for type inference. Defaults to 1000000. You may want to increase it if --optimize is failing."
    )
    stage_parser.add_argument(
        "--gen-schema",
        action="store_true",
        help="(Optional) Generate the table schema automatically. Skip this argument if you're gonna build the schema yourself.",
    )
    stage_parser.add_argument(
        "--drop",
        action="store_true",
        help="If the database and tables are found, drop them and create them again."
    )

    dim_parser.add_argument(
        "--gen-deps",
        action="store_true",
        help="(Optional) Try to automatically infer functional dependencies in the data. "
        "Otherwise, the fdeps.txt file will be used.",
    )
    dim_parser.add_argument(
        "--depsfile",
        type=str,
        help="Name of the .txt file to read or write functional dependencies to. Defaults to ./depsfile.txt",
    )
    args = parser.parse_args()
    if args.cmd == "stage":
        gen_stage(args)
    elif args.cmd == "dmt":
        gen_dmt(args)
