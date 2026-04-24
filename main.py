import argparse
from tabulate import tabulate
from extract import read_sample
from transform import find_functional_dependencies
from load import setup_stage, load_stage


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to generate a SQL Server datamart and dimensions from an Excel source."
    )

    subparsers = parser.add_subparsers(dest="cmd")
    stage_parser = subparsers.add_parser("stage", help="Generation of STAGE database.")
    dim_parser = subparsers.add_parser("dmt", help="Generation of DIM and FACT tables")
    stage_parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="Required. The name of the Excel dataset (without the extension). The file must be located in ./data",
        required=True,
    )
    stage_parser.add_argument(
        "-d",
        "--database-name",
        type=str,
        help="The name of the database. Defaults to dataset name",
    )
    stage_parser.add_argument(
        "-t",
        "--table-name",
        type=str,
        help="The name to use for the tables in the database. Defaults to dataset name",
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
        print("Creating stage schema...")
        if args.gen_schema:
            print(
                "The program can try to match more closely the database fields using the sample data " \
                    "(ej. setting a column to VARCHAR(100) instead of VARCHAR(max)). However, it may also fail."
            )
            opt = input("Try it? (y/N) ")
            opt = True if opt == 'y' else False
            print("Creating schema...")
            setup_stage(
                args.name,
                args.database_name if args.database_name else args.name,
                args.table_name if args.table_name else args.name,
                opt
            )
        print("Loading data...")
        load_stage(
            args.name,
            args.database_name if args.database_name else args.name,
            args.table_name if args.table_name else args.name,
        )

    elif args.cmd == "dmt":
        # Deps
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
            parser.exit()
    # Database
