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
    parser.add_argument('--target_date', type=str, help='Run only specific date')
    parser.add_argument('--parallel', action='store_true', default=True)
    parser.add_argument('--visualize_dag', action='store_true', default=True, help='Save DAG as an image')

    return parser.parse_args()

if __name__ == "__main__":
    
    args = parse_args()

    config_loader = ConfigLoader(args.config_file)
    project_name = config_loader.config_data.get("name", "main")

    logger = setup_logger(project_name, log_file=config_loader.get_log_file(), level=config_loader.get_log_level())

    builder = PipelineBuilder(config_loader, target_date=args.target_date, selected_step=args.step)

    if args.step:
        if args.step not in builder.get_step_names():
            logger.error(f"❌ Step '{args.step}' not defined in DAG.")
            sys.exit(1)

        logger.info(f"Running only step: {args.step}")
        builder.run_step(args.step)

        sys.exit(0)  # ✅ 여기 추가: 단일 step 실행 후 종료

    if args.parallel:
        builder.run_all_parallel(max_workers=4)
    else:
        builder.run_all()

    if args.visualize_dag:
        builder.visualize_dag()