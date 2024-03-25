#!/usr/bin/env python3

import ROOT
import os
import yaml
import logging
import argparse
import csv

logging.basicConfig(format="{levelname:<8s} {message}", style="{", level=logging.INFO)
logger = logging.getLogger()

ROOT.EnableImplicitMT()


def ensure_root_extension(file_name):
    if not file_name.endswith(".root"):
        return file_name + ".root"
    return file_name


class SystematicYieldCalc:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
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
            return config
        except FileNotFoundError:
            logger.error(f"Config file '{config_file}' not found.")
            return None
        except yaml.YAMLError as e:
            logger.error(f"Error while loading config file '{config_file}': {e}")
            return None

    def calculate_yield(self, sample_path, weight_expression, selection):
        df = ROOT.RDataFrame("nominal_Loose", sample_path)
        df = df.Filter(selection)
        df = df.Define("event_weight", weight_expression)
        yield_count = df.Sum("event_weight").GetValue()
        return yield_count

    def process_weight_based_systematic(
        self,
        systematic,
        systematic_yields,
        sample_path,
        nominal_weight,
        adjusted_selection,
    ):
        sys_name = systematic["name"]
        weight_expression_up = f"({nominal_weight})*({systematic['up_weight']})"
        weight_expression_down = f"({nominal_weight})*({systematic['down_weight']})"

        systematic_yields[f"{sys_name}_up"] += self.calculate_yield(
            sample_path, weight_expression_up, adjusted_selection
        )
        systematic_yields[f"{sys_name}_down"] += self.calculate_yield(
            sample_path, weight_expression_down, adjusted_selection
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
                        sys_yield += self.calculate_yield(
                            sample_path, combined_weight, adjusted_selection
                        )

                systematic_yields[f"{systematic['name']}_{variation_type}"] = sys_yield
            else:
                logger.info(
                    f"No '{variation_type}' variation defined for {systematic['name']}."
                )

    def process_flavour(self, base_path, folders, nominal_weight, flavour_config):
        selection = flavour_config["selection"]
        nominal_yield = 0
        systematic_yields = {}

        for systematic in flavour_config["systematics"]:
            if systematic["type"] == "weight":
                systematic_yields[f"{systematic['name']}_up"] = 0
                systematic_yields[f"{systematic['name']}_down"] = 0

        for folder in folders:
            adjusted_selection = selection
            if "boosted" not in folder and "2l_" not in folder:
                adjusted_selection += self.config["extra_selections"]["resolved"]

            for file_rel_path in flavour_config["files"]:

                file_rel_path_with_ext = ensure_root_extension(file_rel_path)

                sample_path = os.path.join(base_path, folder, file_rel_path_with_ext)
                logger.info(f"Processing nominal: {sample_path}")
                nominal_yield += self.calculate_yield(
                    sample_path, nominal_weight, adjusted_selection
                )

                for systematic in flavour_config["systematics"]:
                    if systematic["type"] == "weight":
                        self.process_weight_based_systematic(
                            systematic,
                            systematic_yields,
                            sample_path,
                            nominal_weight,
                            adjusted_selection,
                        )

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

    def run(self):
        base_path = self.config["base_path"]
        folders = self.config["folders"]
        nominal_weight = self.config["nominal_weight"]
        results = {}
        for flavour_name, flavour_config in self.config["flavours"].items():
            logger.info(f"Processing flavour: {flavour_name}")
            nominal_yield, systematic_yields = self.process_flavour(
                base_path, folders, nominal_weight, flavour_config
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
        description="Calculate systematic renormalisation factors"
        "for the ttH(bb) Run-2 Legacy analysis."
    )

    parser.add_argument("config_file", help="Path to the configuration file.")

    parser.add_argument(
        "-o",
        "--output_file",
        help="Path to the output csv file, where the"
        "systematic renormalisation values will be saved.",
    )

    args = parser.parse_args()

    config_file = args.config_file

    output_csv_file = args.output_file

    systematic_yield_calc = SystematicYieldCalc(config_file)
    results = systematic_yield_calc.run()
    for flavour, result in results.items():
        print(f"Flavour: {flavour}")
        for key, value in result.items():
            print(f"  {key}: {value}")

    save_to_csv(results, output_csv_file)
    logger.info(
        f"Systematic renormalisation values have been saved to {output_csv_file}"
    )
