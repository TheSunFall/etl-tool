import argparse
from tabulate import tabulate
from extract import read_sample
from transform import find_functional_dependencies


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to generate a SQL Server datamart and dimensions from an Excel source."
    )
    parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        help="Required. The name of the Excel dataset (without the extension). The file must be located in ./data",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        type=int,
        default=25000,
        help="(Optional, default: 25 000). Number of registers to parse at once. Higher is faster, but will bottleneck depending on RAM.",
    )
    parser.add_argument(
        "--gen-deps",
        action="store_true",
        help="(Optional) Try to automatically infer functional dependencies in the data. "
        "Otherwise, the fdeps.txt file will be used.",
    )
    args = parser.parse_args()
    print("Loading...")

    fdeps = []
    if args.gen_deps:
        sample = read_sample(args.dataset, 1000)
        for col in sample.columns:
            print(type(col))