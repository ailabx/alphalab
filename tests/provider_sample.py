from nanobot.config import load_config
from nanobot.providers.custom_provider import CustomProvider
config = load_config()
custom_provider = config.providers.custom

#provider.chat_with_retry()
async def main():
    # 创建provider实例
    provider = CustomProvider(api_base=custom_provider.api_base, api_key=custom_provider.api_key,
                              default_model=config.agents.defaults.model)

    messages = [
        {
            "role": "user",
            "content": "你好，请问今天天气怎么样？"
        }
    ]

    resp = await provider.chat(messages=messages)

    print(resp)
    print(f"回复内容: {resp.content}")
    print(f"是否有工具调用: {resp.has_tool_calls}")
    print(f"完成原因: {resp.finish_reason}")


# 运行异步函数
import asyncio
asyncio.run(main())