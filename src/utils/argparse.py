import argparse


def arg_parser():

    parser = argparse.ArgumentParser(description="Wikimedia to OpenSearch Pipeline")
    parser.add_argument(
        "--mode",
        choices=["setup", "producer", "consumer", "full"],
        required=True,
        help="Mode to run the application",
    )

    args = parser.parse_args()

    return args
