#!/usr/bin/env python3

import ROOT
import os
import yaml
import logging

logging.basicConfig(format="{levelname:<8s} {message}", style='{', level=logging.INFO)
logger = logging.getLogger()

ROOT.EnableImplicitMT()

class SystematicYieldCalc:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)

    def read_config(self, config_file):
        try:
            with open(config_file, 'r') as f:
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

    def process_flavour(self, base_path, folders, nominal_weight, flavour_config):
        selection = flavour_config['selection']
        nominal_yield = 0
        systematic_yields = {}

        for systematic in flavour_config['systematics']:
            sys_name = systematic['name']
            systematic_yields[f"{sys_name}_up"] = 0
            systematic_yields[f"{sys_name}_down"] = 0

        for folder in folders:
            if "boosted" not in folder:
                selection = selection + self.config['extra_selections']['resolved']
            for file_rel_path in flavour_config['files']:
                sample_path = os.path.join(base_path, folder, file_rel_path)
                logger.info(f"Processing {sample_path}")

                nominal_yield += self.calculate_yield(sample_path, nominal_weight, selection)

                for systematic in flavour_config['systematics']:
                    sys_name = systematic['name']
                    weight_expression_up = f"({nominal_weight})*({systematic['up_weight']})"
                    weight_expression_down = f"({nominal_weight})*({systematic['down_weight']})"

                    systematic_yields[f"{sys_name}_up"] += self.calculate_yield(sample_path, weight_expression_up, selection)
                    systematic_yields[f"{sys_name}_down"] += self.calculate_yield(sample_path, weight_expression_down, selection)

        return nominal_yield, systematic_yields

    def run(self):
        base_path = self.config['base_path']
        folders = self.config['folders']
        nominal_weight = self.config['nominal_weight']
        results = {}
        for flavour_name, flavour_config in self.config['flavours'].items():
            logger.info(f"Processing flavour: {flavour_name}")
            nominal_yield, systematic_yields = self.process_flavour(base_path, folders, nominal_weight, flavour_config)

            renormalisations = {}
            for sys_name, sys_yield in systematic_yields.items():
                renorm = 1 / (sys_yield / nominal_yield) if nominal_yield else 0
                renormalisations[sys_name] = renorm

            results[flavour_name] = {
                "nominal": nominal_yield,
                "systematic_yields": systematic_yields,
                "renormalisations": renormalisations
            }
        return results

if __name__ == '__main__':
    config_file = '/scratch4/levans/Renormalisation_tool/L2_v2.5_25_03_24/config_1l.yaml'
    systematic_yield_calc = SystematicYieldCalc(config_file)
    results = systematic_yield_calc.run()
    for flavour, result in results.items():
        print(f"Flavour: {flavour}")
        for key, value in result.items():
            print(f"  {key}: {value}")