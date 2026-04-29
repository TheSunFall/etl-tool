import argparse
import json
from tabulate import tabulate
from extract import read_sample
from transform import find_functional_dependencies
from load import setup_stage, load_stage


def load_config():
    """Load the dataset configuration from etl.stage.json"""
    with open("etl.stage.json", "r") as f:
        return json.load(f)


def get_dataset_config(config, dataset_name):
    """Get configuration for a specific dataset"""
    for dataset in config["datasets"]:
        if dataset["dataset"] == dataset_name or dataset["dataset"].replace(".", "_").replace("-", "_").lower() == dataset_name.lower():
            return dataset
    raise ValueError(f"Dataset '{dataset_name}' not found in configuration")


def process_single_dataset(dataset_name, dataset_config, args):
    """Process a single dataset for staging"""
    print(f"\nProcessing dataset: {dataset_name}")
    if args.gen_schema:
        print("Creating schema...")
        setup_stage(
            dataset_name,
            args.database_name if args.database_name else dataset_config["database"],
            args.table_name if args.table_name else dataset_config["tablename"],
            args.schema if args.schema != "dbo" else dataset_config["schema"],
            args.sample_size,
            args.optimize
        )
    print("Loading data...")
    load_stage(
        dataset_name,
        args.database_name if args.database_name else dataset_config["database"],
        args.table_name if args.table_name else dataset_config["tablename"],
        args.schema if args.schema != "dbo" else dataset_config["schema"]
    )


def gen_stage(args):
    config = load_config()
    
    # If --all flag is set, process all datasets
    if args.all:
        print(f"Processing all {len(config['datasets'])} datasets...")
        for dataset in config["datasets"]:
            try:
                process_single_dataset(dataset["dataset"], dataset, args)
            except Exception as e:
                print(f"Error processing {dataset['dataset']}: {e}")
                continue
        print("\nAll datasets processed!")
        return
    
    # Process single dataset
    if not args.name:
        print("Error: Either -n/--name or --all must be provided")
        return
    
    try:
        dataset_config = get_dataset_config(config, args.name)
    except ValueError as e:
        print(f"Error: {e}")
        print("Available datasets:")
        for ds in config["datasets"]:
            print(f"  - {ds['dataset']}")
        return
    
    print("Creating stage...")
    process_single_dataset(args.name, dataset_config, args)


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
    dataset_names = [ds["dataset"] for ds in config["datasets"]]
    
    parser = argparse.ArgumentParser(
        description="Tool to generate a SQL Server Datamart and dimensions from an Excel or .parquet source."
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
        default=1000,
        help="Size of the sample for type inference. Defaults to 1000. You may want to increase it if --optimize is failing."
    )
    stage_parser.add_argument(
        "--gen-schema",
        action="store_true",
        help="(Optional) Generate the table schema automatically. Skip this argument if you're gonna build the schema yourself.",
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
