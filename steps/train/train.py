# steps/train/train.py
import sys, json
import argparse
import os
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger
from steps.train.queries.train_dataset_etl_query import generate_train_dataset_etl_query

def training_needed():
    return True

def parse_args():
    parser = argparse.ArgumentParser(description="Step: train")
    parser.add_argument('--config_file', type=str, required=True)
    parser.add_argument('--global_config_file', type=str, required=False)
    parser.add_argument('--target_date', type=str, required=True)
    return parser.parse_args()

if __name__ == "__main__":
    if not training_needed():
        print(json.dumps({"skipped": True}))
        sys.exit(0)

    args = parse_args()
    config_loader = ConfigLoader(args.config_file, validate=False)
    train_config = config_loader.config_data.get("config", {})

    global_config_path = (
        args.global_config_file
        or os.environ.get("GLOBAL_CONFIG")
        or "configs/config.yaml"
    )
    global_loader = ConfigLoader(global_config_path, validate=False)
    global_config = global_loader.get_global_config()


    step_name = train_config.get("name", "train")
    logger = setup_logger(
        name=step_name,
        log_file=global_loader.get_log_file(step_name),
        level=global_loader.get_log_level(),
        stream_to_stdout=True  # 반드시 stdout
    )

    logger.info(f"Training config loaded: {train_config}")
    logger.info(f"[INFO] Global config: env={global_config.env}, db={global_config.db}, s3={global_config.s3.base_output}")
    logger.info(f"[INFO] Step config: {json.dumps(train_config, indent=2)}")

    query = generate_train_dataset_etl_query(cfg=global_config, target_date=args.target_date)
    logger.info(f"[QUERY]\n{query}")

    print(json.dumps({"success": True}))