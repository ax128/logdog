"""最小 LangChain Chat 请求脚本（需网络与 API Key）。

安装::

    uv sync --extra langchain

运行::

    set OPENAI_API_KEY=...
    set OPENAI_BASE_URL=https://你的网关/v1
    python tests/llm/langchain_chat_smoke.py

``OPENAI_BASE_URL``：自定义 OpenAI 兼容服务的根地址（通常以 ``/v1`` 结尾，与网关文档一致）。
也可用 ``LANGCHAIN_OPENAI_BASE_URL``，二者任一即可（前者优先）。
"""

from __future__ import annotations

import os
import sys


def _openai_base_url() -> str | None:
    raw = (os.environ.get("OPENAI_BASE_URL") or os.environ.get("LANGCHAIN_OPENAI_BASE_URL") or "").strip()
    return raw or None


def main() -> int:
    try:
        from langchain_core.messages import HumanMessage
        from langchain_openai import ChatOpenAI
    except ImportError:
        print(
            "缺少依赖：请执行 uv sync --extra langchain（或 pip install langchain-openai）",
            file=sys.stderr,
        )
        return 1

    if not os.environ.get("OPENAI_API_KEY"):
        print("请设置环境变量 OPENAI_API_KEY", file=sys.stderr)
        return 1

    base_url = _openai_base_url()
    if not base_url:
        print(
            "请设置自定义接口地址：OPENAI_BASE_URL（或 LANGCHAIN_OPENAI_BASE_URL），"
            "例如 https://api.example.com/v1",
            file=sys.stderr,
        )
        return 1

    model = os.environ.get("LANGCHAIN_CHAT_MODEL", "gpt-4o-mini")
    llm = ChatOpenAI(model=model, temperature=0, base_url=base_url)
    msg = HumanMessage(content='请只回复一个词："ok"')
    out = llm.invoke([msg])
    text = getattr(out, "content", str(out))
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
