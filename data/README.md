# Data

`sample/netflow_sample.csv` is a 1,000-row excerpt (868 benign / 132 attack flows) of the public
**NF-CSE-CIC-IDS2018-v2** NetFlow dataset, enough to run the pipeline end to end.

For meaningful results download the full dataset (about 18.9 M flows) from the
[UQ NetFlow NIDS collection](https://staff.itee.uq.edu.au/marius/NIDS_datasets/) and point the jobs at it:

```bash
DATASET_CSV=/app/data/NF-CSE-CIC-IDS2018-v2.csv make train
```

Expected columns: `IPV4_SRC_ADDR`, `IPV4_DST_ADDR`, the 41 feature columns listed in
`flowids/features.py`, `Label` (0/1) and `Attack` (family name).
