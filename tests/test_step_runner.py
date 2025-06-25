# tests/test_step_runner.py

import unittest
import subprocess  # subprocess 모듈 임포트 추가
from unittest.mock import patch, MagicMock
from pipeline.step_runner import StepRunner
from typing import Any  # Any 임포트 추가

class TestStepRunner(unittest.TestCase):
    def setUp(self):
        """테스트 전에 실행할 준비 작업"""
        self.step = StepRunner(name="step_a", script_path="steps/a.py", config_path="configs/a.yaml")

    @patch("subprocess.run")  # subprocess.run을 mocking
    def test_run_subprocess_success(self, mock_run):
        """subprocess.run이 성공하는 경우 테스트"""
        # subprocess.run이 성공적인 종료 코드를 반환하도록 mock 설정
        mock_run.return_value = MagicMock(returncode=0)
        
        result = self.step.run_subprocess()
        
        # 결과가 True여야 한다.
        self.assertTrue(result)
        # 호출된 subprocess.run의 인자들 확인
        mock_run.assert_called_once_with(
            ["python", "steps/a.py", "--config_file", "configs/a.yaml"],
            check=True,
            env=unittest.mock.ANY  # 환경 변수는 구체적으로 검증하지 않고, env 인자가 존재하는지 확인
        )

    @patch("subprocess.run")
    def test_run_subprocess_failure(self, mock_run):
        """subprocess.run이 실패하는 경우 테스트"""
        # subprocess.run이 예외를 발생시키도록 설정
        mock_run.side_effect = subprocess.CalledProcessError(1, "python")
        
        result = self.step.run_subprocess()
        
        # 결과가 False여야 한다.
        self.assertFalse(result)

    @patch("subprocess.run")
    def test_run_subprocess_retry(self, mock_run):
        """subprocess.run이 실패하고 재시도하는지 테스트"""
        # subprocess.run이 첫 번째 시도에서 실패하고, 두 번째 시도에서 성공하도록 설정
        mock_run.side_effect = [subprocess.CalledProcessError(1, "python"), MagicMock(returncode=0)]
        
        result = self.step.run_subprocess()
        
        # 결과가 True여야 한다.
        self.assertTrue(result)
        self.assertEqual(mock_run.call_count, 2)  # 재시도가 2번 이루어졌는지 확인

if __name__ == "__main__":
    unittest.main()