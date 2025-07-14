# steps/settings.py

from dataclasses import dataclass
from typing import Dict

@dataclass
class S3Paths:
    base_output: str
    tmp_output: str

@dataclass
class AthenaTables:
    customer: str
    history: str

@dataclass
class GlobalConfig:
    env: str
    db: str
    workgroup: str
    s3: S3Paths
    athena: AthenaTables

    @staticmethod
    def from_dict(data: Dict) -> "GlobalConfig":
        return GlobalConfig(
            env=data["env"],
            db=data["db"],
            workgroup=data["workgroup"],
            s3=S3Paths(**data["s3"]),
            athena=AthenaTables(**data["athena"]["tables"]),
        )
