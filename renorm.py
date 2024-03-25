#!/usr/bin/env python3

import ROOT
import os
import yaml
import logging
import argparse
import csv
import multiprocessing
import logging


ROOT.EnableImplicitMT()


def ensure_root_extension(file_name):
    """Helper function to add .root extension if missing"""
    if not file_name.endswith(".root"):
        return file_name + ".root"
    return file_name


class GreenFormatter(logging.Formatter):
    GREEN = "\033[1;32m"
    RESET = "\033[1;0m"

    def format(self, record):
        # log flavour processing in green
        if record.levelno == logging.INFO and record.msg.startswith(
            "Processing flavour:"
        ):
            record.msg = f"{self.GREEN}{record.msg}{self.RESET}"
        return super().format(record)


# logging configuration
handler = logging.StreamHandler()
handler.setFormatter(GreenFormatter("{levelname:<8s} {message}", style="{"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


class YieldResult:
    def __init__(self):
        self.yields = {}

    def merge(self, other):
        for sys_name, yield_value in other.yields.items():
            self.yields[sys_name] = self.yields.get(sys_name, 0) + yield_value


class SystematicYieldCalc:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
        if self.config is None:
            raise ValueError("Failed to load configuration.")
        if not self.validate_config(self.config):
            raise ValueError("Configuration validation failed.")

    def validate_config(self, config):
        required_keys = ["base_path", "folders", "nominal_weight", "flavours"]
        for key in required_keys:
            if key not in config:
                logger.error(f"Missing required configuration key: '{key}'")
                return False
        return True

    def read_config(self, config_file):
        try:
            with open(config_file, "r") as f:
                config = yaml.safe_load(f)
            if config is None:
                logger.error(f"Configuration file '{config_file}' is empty.")
                return None
            return config
        except FileNotFoundError:
            logger.error(f"Config file '{config_file}' not found.")
            return None
        except yaml.YAMLError as e:
            logger.error(f"Error while loading config file '{config_file}': {e}")
            return None

    def calculate_yields(self, df, weight_expressions, selection):
        def fill_result(result, weight_name, weight_expr):
            result.yields[weight_name] = (
                df.Define(f"weight_{weight_name}", weight_expr)
                .Sum(f"weight_{weight_name}")
                .GetValue()
            )

        result = YieldResult()
        df = df.Filter(selection)
        for name, weight_expr in weight_expressions.items():
            fill_result(result, name, weight_expr)
        return result

    def process_weight_based_systematic(self, systematic, weight_expressions):
        sys_name = systematic["name"]
        weight_expressions[f"{sys_name}_up"] = (
            f"({weight_expressions['nominal']})*({systematic['up_weight']})"
        )
        weight_expressions[f"{sys_name}_down"] = (
            f"({weight_expressions['nominal']})*({systematic['down_weight']})"
        )

    def process_sample_based_systematic(
        self,
        systematic,
        systematic_yields,
        base_path,
        folders,
        nominal_weight,
        selection,
        flavour_config,
    ):
        for variation_type in ["up", "down"]:
            variation_key = f"{variation_type}_files"
            if variation_key in systematic:
                sys_yield = 0

                additional_weight = systematic.get(f"{variation_type}_weight", "1")
                combined_weight = f"({nominal_weight})*({additional_weight})"

                for file_rel_path in systematic[variation_key]:
                    file_rel_path_with_ext = ensure_root_extension(file_rel_path)
                    for folder in folders:
                        adjusted_selection = selection
                        if "boosted" not in folder:
                            adjusted_selection += self.config["extra_selections"][
                                "resolved"
                            ]

                        sample_path = os.path.join(
                            base_path, folder, file_rel_path_with_ext
                        )
                        logger.info(
                            f"Processing {variation_type} variation for {systematic['name']}: {sample_path}"
                        )
                        df = ROOT.RDataFrame("nominal_Loose", sample_path)
                        result = self.calculate_yields(
                            df, {"nominal": combined_weight}, adjusted_selection
                        )
                        sys_yield += result.yields["nominal"]
                systematic_yields[f"{systematic['name']}_{variation_type}"] = sys_yield
            else:
                logger.info(
                    f"No '{variation_type}' variation defined for {systematic['name']}."
                )

    def process_flavour(self, base_path, folders, nominal_weight, flavour_config):
        selection = flavour_config["selection"]
        weight_expressions = {"nominal": nominal_weight}
        for systematic in flavour_config["systematics"]:
            if systematic["type"] == "weight":
                self.process_weight_based_systematic(systematic, weight_expressions)

        result = YieldResult()
        for folder in folders:
            adjusted_selection = selection
            if "boosted" not in folder and "2l_" not in folder:
                adjusted_selection += self.config["extra_selections"]["resolved"]

            for file_rel_path in flavour_config["files"]:
                file_rel_path_with_ext = ensure_root_extension(file_rel_path)
                sample_path = os.path.join(base_path, folder, file_rel_path_with_ext)

                logger.info(
                    f"Processing nominal and weight-based systematics: {sample_path}"
                )
                df = ROOT.RDataFrame("nominal_Loose", sample_path)
                result.merge(
                    self.calculate_yields(df, weight_expressions, adjusted_selection)
                )

        systematic_yields = result.yields
        nominal_yield = systematic_yields.pop("nominal", 0)

        for systematic in flavour_config["systematics"]:
            if systematic["type"] == "sample":
                self.process_sample_based_systematic(
                    systematic,
                    systematic_yields,
                    base_path,
                    folders,
                    nominal_weight,
                    selection,
                    flavour_config,
                )

        return nominal_yield, systematic_yields

    def process_flavour_wrapper(self, args):
        flavour_name, flavour_config = args
        logger.info(f"Processing flavour: {flavour_name}")
        return self.process_flavour(
            self.config["base_path"],
            self.config["folders"],
            self.config["nominal_weight"],
            flavour_config,
        )

    def run(self, use_multiprocessing=False):
        results = {}
        if use_multiprocessing is True:
            with multiprocessing.Pool() as pool:
                flavour_results = pool.map(
                    self.process_flavour_wrapper,
                    self.config["flavours"].items(),
                )
                for flavour_name, (nominal_yield, systematic_yields) in zip(
                    self.config["flavours"].keys(), flavour_results
                ):
                    renormalisations = {}
                    for sys_name, sys_yield in systematic_yields.items():
                        renorm = 1 / (sys_yield / nominal_yield) if nominal_yield else 0
                        renormalisations[sys_name] = renorm

                    results[flavour_name] = {
                        "nominal": nominal_yield,
                        "systematic_yields": systematic_yields,
                        "renormalisations": renormalisations,
                    }
        else:
            for flavour_name, flavour_config in self.config["flavours"].items():
                logger.info(f"Processing flavour: {flavour_name}")
                nominal_yield, systematic_yields = self.process_flavour(
                    self.config["base_path"],
                    self.config["folders"],
                    self.config["nominal_weight"],
                    flavour_config,
                )
                renormalisations = {}
                for sys_name, sys_yield in systematic_yields.items():
                    renorm = 1 / (sys_yield / nominal_yield) if nominal_yield else 0
                    renormalisations[sys_name] = renorm

                results[flavour_name] = {
                    "nominal": nominal_yield,
                    "systematic_yields": systematic_yields,
                    "renormalisations": renormalisations,
                }
        return results


def save_to_csv(results, output_file):
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        header = [
            "Flavour",
            "Systematic",
            "Nominal yield",
            "Systematic yield (up)",
            "Systematic yield (down)",
            "Renorm. value (up)",
            "Renorm. value (down)",
        ]
        writer.writerow(header)

        for flavour, result in results.items():
            nominal_yield = result["nominal"]
            systematics_processed = set()

            for sys_name, sys_yield in result["systematic_yields"].items():
                base_sys_name = sys_name.rsplit("_", 1)[0]

                if base_sys_name in systematics_processed:
                    continue

                systematics_processed.add(base_sys_name)

                up_yield = result["systematic_yields"].get(f"{base_sys_name}_up", "N/A")
                down_yield = result["systematic_yields"].get(
                    f"{base_sys_name}_down", "N/A"
                )

                renorm_up = (
                    "N/A"
                    if up_yield == "N/A"
                    else 1 / (float(up_yield) / nominal_yield)
                )
                renorm_down = (
                    "N/A"
                    if down_yield == "N/A"
                    else 1 / (float(down_yield) / nominal_yield)
                )

                writer.writerow(
                    [
                        flavour,
                        base_sys_name,
                        nominal_yield,
                        up_yield,
                        down_yield,
                        renorm_up if renorm_up != "N/A" else "N/A",
                        renorm_down if renorm_down != "N/A" else "N/A",
                    ]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate systematic renormalisation factors for the ttH(bb) Run-2 Legacy analysis."
    )

    parser.add_argument("config_file", help="Path to the configuration file.")

    parser.add_argument(
        "-o",
        "--output_file",
        help="Path to the output csv file, where the systematic renormalisation values will be saved.",
    )

    parser.add_argument(
        "--systematics",
        nargs="+",
        default=None,
        help="List of systematics to run over (default: all systematics)"
        "where name of systematic is the same as in the config file",
    )

    parser.add_argument(
        "--flavours",
        nargs="+",
        default=None,
        help="List of flavours to run over (default: all flavours)"
        "where name of flavour is the same as in the config file",
    )

    parser.add_argument(
        "--multiprocessing",
        action="store_true",
        help="Use multi-processing to calculate yields (default: False)"
        "This will run all flavours in parallel and speeds up the"
        "calculation, but beward the use of your computational resources"
        "as it will use all available cores.",
    )

    args = parser.parse_args()

    config_file = args.config_file
    output_csv_file = args.output_file
    systematics_to_run = args.systematics
    flavours_to_run = args.flavours
    use_multiprocessing = args.multiprocessing

    systematic_yield_calc = SystematicYieldCalc(config_file)

    # filter systematics
    if systematics_to_run is not None:
        for flavour_config in systematic_yield_calc.config["flavours"].values():
            flavour_config["systematics"] = [
                systematic
                for systematic in flavour_config["systematics"]
                if systematic["name"] in systematics_to_run
            ]
    # filter flavours
    if flavours_to_run is not None:
        systematic_yield_calc.config["flavours"] = {
            flavour: config
            for flavour, config in systematic_yield_calc.config["flavours"].items()
            if flavour in flavours_to_run
        }

    if use_multiprocessing is True:
        logger.info("Using multi-processing to calculate yields.")

    logging.info("Running over the following flavours:")
    for flavour in systematic_yield_calc.config["flavours"]:
        logging.info(f"- {flavour}")

    logging.info("\nRunning over the following systematics:")
    all_systematics = set()
    for flavour_config in systematic_yield_calc.config["flavours"].values():
        all_systematics.update(
            systematic["name"] for systematic in flavour_config["systematics"]
        )
    for systematic in all_systematics:
        logging.info(f"- {systematic}")

    print("\nStarting renormalisation calculation...")

    results = systematic_yield_calc.run()

    for flavour, result in results.items():
        print(f"Flavour: {flavour}")
        for key, value in result.items():
            print(f"  {key}: {value}")

    save_to_csv(results, output_csv_file)
    logger.info(
        f"Systematic renormalisation values have been saved to {output_csv_file}"
    )
