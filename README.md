## Systematic Renormalisation Tool for ttH(bb) Legacy Run-2 Analysis

This is a small utility tool to facilitate the calculation of the fidicual cross-section renormalisation values for systematic samples in the ttH(bb) full Run 2 legacy analysis. It makes use of ROOT and RDataFrames for somewhat efficient data-processing.

### Usage

#### The configuration file:

First, you will need to prepare a configuration file specifying all nominal samples and systematic samples (or weight strings).

The script expects the following keys in the configuration file:

- `base_path:` The base path to your samples
- `folders:` The folder(s) in which your samples live inside within the base path
- `nominal_weight:` The nominal event-weight string used for the analysis
- `flavours:` The $t\bar{t}$ flavour components you would like to run over

Beyond this, you can also specifiy:

-`extra_selections:` any additional selections you would like to apply. You can name and put these extra selections inside this block. You will though currently have to then implement the logic for these selections inside the script. An example for applying a boosted event veto can be found in `L2_v2.5_25_03_24/combined.yaml`, where this selection is called `resolved`

Inside each `flavour` block, you can specify:

- `Selection:` The selection string to be applied in case specific heavy-flavour component selection is required. Other selections can also be appended here if needed, e.g kinematic cuts like `nJets == 5`, etc.
- `files:` These are the nominal samples used for the calculation
- `Systematics:`
  - `name:` The name of the systematic variation
  - `type:` The type of systematic variation to process (can be of type weight or sample)
  - `up_files:` The list of up associated systematic sampes (type: sample)
  - `down_files:` The list of down associated systematic sampes (type: sample)
  - `up_weight:` The weight string for the up variation (type: both)
    **Note**: This will be mulitiplied with the nominal weight string, and in the case of sample-type systematics, can be used to apply additional weight to the samples, e.g the $H_{T}$`weight_ht_reweight_nominal` weights
  - `down_weight:`The weight string for the down variation (type: both, as above)

As stated above, an example of the structure of a configuration file can be found at `L2_v2.5_25_03_24/combined.yaml`. File names do not need to append `.root`, the script will take care of this for you in case you forget.

#### Running the code and run options:

For a simple run, simply do:

```
./renorm.py /path/to/config.yaml
```

The script comes configured with multiple running options, these being:

| Option            | Description                                                                                                                             |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| -o, --ouput_file  | The path to a .csv file in which to save the nominal and systematic yields alongside the renormalisation values.                        |
| --systematics     | A space-separated list of systematics to run over (default: all systematics) where name of systematic is the same as in the config file |
| --flavours        | A space-separated list of flavours to run over (default: all flavours) where name of flavour is the same as in the config file          |
| --multiprocessing | Run over flavours supplied simulatenously for somewhat quicker processing                                                               |

In additon to the output .csv file, the yields and renormalisation values will also be printed to the terminal. Inside the csv file you will find:

- `Flavour:`,`Systematic:`,`Nominal yield:`, `Syst yield (up/down)`, `Renorm. value (up/down)`

## Notes:

- The script has the tree to be used hard-coded (i.e `nominal_Loose`)
