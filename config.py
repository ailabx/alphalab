import os
import sys
import toml
from pathlib import Path
from typing import Dict, Any
import platform

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR.joinpath('data')
DATA_DIR.mkdir(exist_ok=True)

class Config:
    """跨平台配置管理类"""

    def __init__(self):
        # 基础配置项
        self.MODEL: str = "Qwen3-32B"
        self.BASE_URL: str = "http://IP:PORT/v1"
        self.KEY: str = ""
        self.TS_TOKEN: str = ""
        self.TQ_USERNAME : str = ""
        self.TQ_PASSWORD : str = ""

        # 操作系统检测
        self.system = platform.system().lower()

        # 确定配置目录路径
        self._config_dir = self._get_config_dir()
        self._config_file = self._config_dir / "config.toml"

        # 初始化配置
        self._init_config()

    def _get_config_dir(self) -> Path:
        """获取跨平台的配置目录"""
        home_dir = Path.home()

        # 所有系统都使用 ~/.aitrader 目录
        config_dir = home_dir / ".alphalab"

        return config_dir

    def _init_config(self) -> None:
        """初始化配置系统"""
        # 1. 首先确保配置目录存在
        self._create_config_dir()

        # 2. 然后加载或创建配置文件
        if self._config_file.exists():
            self._load_config()
        else:
            self._create_config_file()

    def _create_config_dir(self) -> None:
        """创建配置目录"""
        try:
            if not self._config_dir.exists():
                self._config_dir.mkdir(parents=True, exist_ok=True)
                print(f"已创建配置目录: {self._config_dir}")
        except Exception as e:
            print(f"创建配置目录失败: {e}")
            sys.exit(1)

    def _create_config_file(self) -> None:
        """创建配置文件"""
        default_config = {
            "MODEL": self.MODEL,
            "BASE_URL": self.BASE_URL,
            "KEY": self.KEY,
            "TS_TOKEN": self.TS_TOKEN,
            "TQ_USERNAME": self.TQ_USERNAME,
            "TQ_PASSWORD": self.TQ_PASSWORD
        }

        try:
            with open(self._config_file, 'w', encoding='utf-8') as f:
                toml.dump(default_config, f)
            print(f"已创建配置文件: {self._config_file}")
        except Exception as e:
            print(f"创建配置文件失败: {e}")
            sys.exit(1)

    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            with open(self._config_file, 'r', encoding='utf-8') as f:
                config_data = toml.load(f)

            # 更新配置值
            self.MODEL = config_data.get("MODEL", self.MODEL)
            self.BASE_URL = config_data.get("BASE_URL", self.BASE_URL)
            self.KEY = config_data.get("KEY", self.KEY)
            self.TS_TOKEN = config_data.get("TS_TOKEN", self.TS_TOKEN)
            self.TQ_USERNAME = config_data.get("TQ_USERNAME", self.TQ_USERNAME)
            self.TQ_PASSWORD = config_data.get("TQ_PASSWORD", self.TQ_PASSWORD)

        except Exception as e:
            print(f"加载配置文件失败: {e}")
            print("使用默认配置")

    def update_config(self, **kwargs) -> None:
        """更新配置并保存到文件"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                print(f"警告: 未知配置项 '{key}'")

        self._save_config()

    def _save_config(self) -> None:
        """保存配置到文件"""
        config_data = {
            "MODEL": self.MODEL,
            "BASE_URL": self.BASE_URL,
            "KEY": self.KEY,
            "TS_TOKEN": self.TS_TOKEN
        }

        try:
            with open(self._config_file, 'w', encoding='utf-8') as f:
                toml.dump(config_data, f)
        except Exception as e:
            print(f"保存配置失败: {e}")
            raise

    def reload(self) -> None:
        """重新加载配置文件"""
        self._load_config()

    def get_all(self) -> Dict[str, Any]:
        """获取所有配置"""
        return {
            "MODEL": self.MODEL,
            "BASE_URL": self.BASE_URL,
            "KEY": self.KEY,
            "TS_TOKEN": self.TS_TOKEN,
            "TQ_USERNAME": self.TQ_USERNAME,
            "TQ_PASSWORD": self.TQ_PASSWORD
        }

    def get_config_path(self) -> Dict[str, str]:
        """获取配置文件路径信息"""
        return {
            "系统": platform.system(),
            "配置文件": str(self._config_file)
        }


# 创建全局配置实例
Config = Config()

# 为了方便导入，也可以创建别名
config = Config

# 测试代码
if __name__ == "__main__":
    # 获取路径信息
    paths = Config.get_config_path()
    print(f"系统: {paths['系统']}")
    print(f"配置文件: {paths['配置文件']}")

    # 访问配置
    print(f"\n当前配置:")
    print(f"模型: {config.MODEL}")
    print(f"API地址: {config.BASE_URL}")
    print(f"API密钥: {config.KEY}")
    print(f"TS_TOKEN: {config.TS_TOKEN}")

