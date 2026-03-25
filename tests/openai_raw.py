import asyncio
from openai import AsyncOpenAI

from nanobot.config import load_config
from nanobot.providers.custom_provider import CustomProvider
config = load_config()
custom_provider = config.providers.custom

#provider.chat_with_retry()


# 配置客户端指向兼容 OpenAI 的 API 服务
client = AsyncOpenAI(
    base_url=custom_provider.api_base,  # 替换为你的 API 地址
    api_key=custom_provider.api_key,  # 如有需要，填入有效 key（有些服务可填任意值）
)


async def main():
    try:
        import time
        # 非流式请求（stream=False 为默认值，可省略）
        start = time.perf_counter()



        response = await client.chat.completions.create(
            model=config.agents.defaults.model,  # 指定模型名称（根据服务实际情况填写）
            messages=[
                # {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "中国首都是哪里？"}
            ],
            temperature=0.7,
            max_tokens=500,
            stream=False  # 显式指定非流式（默认就是 False）
        )
        elapsed = time.perf_counter() - start
        print(f"chat 耗时: {elapsed:.3f} 秒")
        print(response)

        # 提取并打印回复内容
        reply = response.choices[0].message.content
        print("回复内容：", reply)

        # 可选：查看完整响应结构
        # print(response)

    except Exception as e:
        print(f"请求出错：{e}")


# 运行异步主函数
if __name__ == "__main__":
    asyncio.run(main())