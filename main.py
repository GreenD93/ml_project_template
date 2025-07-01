# main.py
import argparse
import sys
from pipeline.config_loader import ConfigLoader
from pipeline.pipeline_builder import PipelineBuilder
from pipeline.logger import setup_logger

def parse_args():
    parser = argparse.ArgumentParser(description="ML Workflow")
    parser.add_argument('--config_file', type=str, required=True, default='configs/config.yaml')
    parser.add_argument('--step', type=str, help='Run only specific step')
    parser.add_argument('--parallel', action='store_true', default=True)

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    config_loader = ConfigLoader(args.config_file)
    logger = setup_logger("main", log_file=config_loader.get_log_file(), level=config_loader.get_log_level())

    builder = PipelineBuilder(config_loader)

    if args.step:
        if args.step not in builder.get_step_names():
            logger.error(f"❌ Step '{args.step}' not defined in DAG.")
            sys.exit(1)
        logger.info(f"Running only step: {args.step}")
        builder.run_step(args.step)

    if args.parallel:
        builder.run_all_parallel(max_workers=4)
    else:
        builder.run_all()