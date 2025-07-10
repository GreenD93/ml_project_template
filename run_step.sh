#!/bin/bash

# 기본 config.yaml 경로
GLOBAL_CONFIG="configs/config.yaml"

# 인자로 config_file 경로가 전달되었으면 그 경로를 사용하고, 아니면 기본값 사용
CONFIG_FILE="${1:-$GLOBAL_CONFIG}"

# config.yaml에서 프로젝트 경로 가져오기 (여기서부터 경로 처리 시작)
PROJECT_DIR=$(python -c "import yaml; print(yaml.safe_load(open('$CONFIG_FILE'))['paths']['project_dir'])")

# PYTHONPATH 환경 변수 설정 (여기서 pipeline/ 디렉토리를 PYTHONPATH로 추가해야 함)
export PYTHONPATH=$PROJECT_DIR/pipeline:$PYTHONPATH  # pipeline 디렉토리를 PYTHONPATH로 추가

# Python 스크립트를 실행, config_file 인자 전달
python main.py --config_file $CONFIG_FILE --target_date 20250523 --step train