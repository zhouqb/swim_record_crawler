# swim_record_crawler
A crawler for swimmer best personal results

# Installation
1. [Optional] Install [miniconda](https://docs.conda.io/projects/miniconda/en/latest/miniconda-install.html) and create an environment
```bash
conda create --name swim_record_crawler python=3.11
conda activate swim_record_crawler 
```
2. Download the repo and install `requirments.txt`.
```bash
git clone https://github.com/zhouqb/swim_record_crawler
cd swim_record_crawler 
pip install -r requirements.txt
```

# Run
Prepare a swimmer id `xlsx`, which has three columns, `name`, `swimclub_id` and `swimming_rank_id`. Then run
```bash
python crawl_swim_record.py --input <input_file_path> --output <output xlsx path>
```