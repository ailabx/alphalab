from nanobot.agent import loop
from nanobot.agent.loop import AgentLoop
from nanobot.bus import MessageBus

from nanobot.agent.loop import AgentLoop
from nanobot.bus.queue import MessageBus
from nanobot.config.paths import get_cron_dir


from nanobot.config import load_config
from nanobot.providers.custom_provider import CustomProvider


async def main():
    config = load_config()
    custom_provider = config.providers.custom

    bus = MessageBus()
    provider = CustomProvider(api_base=custom_provider.api_base, api_key=custom_provider.api_key,
                                  default_model=config.agents.defaults.model)

    agent_loop = AgentLoop(
        bus=bus,
        provider=provider,
        workspace=config.workspace_path,
        model=config.agents.defaults.model,

    )
    resp = await agent_loop.process_direct('etf最近20天涨幅最高的10支？')
    print(resp)
    return resp

# 运行异步函数
import asyncio
asyncio.run(main())