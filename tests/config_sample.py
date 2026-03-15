from nanobot.config import load_config

config = load_config()
print(config.agents.defaults.model)
print(config.providers.custom)