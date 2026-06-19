"""AI Tutor.

Provides a Socratic coding mentor. If an LLM provider is configured
(``SQ_TUTOR_PROVIDER`` = anthropic | openai | huggingface) it calls that
provider; otherwise it falls back to a fast, fully-offline rule-based tutor so
the public demo is useful with zero secrets and zero cost.
"""
from __future__ import annotations

import re

import httpx

from app.config import get_settings

SYSTEM_PROMPT = (
    "You are SparkQuest's friendly tutor for absolute beginners learning Python, "
    "PySpark, and Spark Structured Streaming. Be encouraging and concise. Prefer "
    "Socratic hints and conceptual nudges over full solutions. Only give a complete "
    "code answer if the learner explicitly asks for the solution. When the learner "
    "shows an error, explain in plain language what it means and the single most "
    "likely fix. Keep replies under ~150 words and use short code snippets only when "
    "they clarify a point."
)

# Common error signatures -> beginner-friendly guidance (offline fallback).
_ERROR_HINTS: list[tuple[str, str]] = [
    (r"IndentationError", "Python is picky about indentation. Make sure code inside loops, "
     "functions, or `if` blocks is indented consistently (4 spaces)."),
    (r"SyntaxError", "There's a typo in the code's structure — often a missing colon `:`, "
     "bracket, or quote. Look at the line the error points to and the line just above it."),
    (r"NameError", "You're using a name that doesn't exist yet. Check for a typo, or make "
     "sure you defined/assigned the variable before using it."),
    (r"TypeError", "An operation got the wrong *type* of value — e.g. adding a string to a "
     "number. Print the values to see what types you actually have."),
    (r"AttributeError", "You called a method that doesn't exist on that object. Double-check "
     "the spelling, and that the object is the type you expect (e.g. a DataFrame vs a list)."),
    (r"(AnalysisException|cannot resolve|Column.*does not exist)",
     "Spark can't find a column you referenced. Check the exact column name and capitalisation "
     "with `df.printSchema()` — names are case-sensitive."),
    (r"Py4JJavaError", "The Spark job failed while executing. This usually points to a bad "
     "transformation — verify column names, types, and that your aggregation is valid."),
    (r"AnalysisException.*streaming",
     "For streaming queries, remember some operations need a watermark, and you must call "
     "`.writeStream` (not `.write`) and `.start()` to launch the query."),
]


def _fallback_reply(brief: str, hints: list[str], concepts: list[str], code: str, question: str, last_error: str) -> str:
    if last_error:
        for pattern, guidance in _ERROR_HINTS:
            if re.search(pattern, last_error):
                return f"Let's read that error together. {guidance}\n\nTip: isolate the problem by " \
                       "printing intermediate values right before the failing line."
        return ("I see an error. Read it bottom-to-top: the last line names the error type, and the "
                "line number points at where Python gave up. Try printing the values involved just "
                "before that line to see what's really happening.")
    if question:
        focus = concepts[0] if concepts else "this step"
        hint = hints[0] if hints else "Re-read the brief and identify the one transformation it asks for."
        return (f"Good question. For {focus}, here's a nudge: {hint} "
                "Try it, run your code, and tell me what output you get — we'll go from there.")
    if hints:
        return f"Here's a hint to get unstuck: {hints[0]}"
    return ("Break the task into the smallest first step, write just that, and run it. Small, "
            "verifiable steps beat one big leap. What's the very first thing the brief asks for?")


def _build_user_message(brief: str, code: str, question: str, last_error: str) -> str:
    parts = [f"Challenge brief:\n{brief.strip()}"]
    if code.strip():
        parts.append(f"My current code:\n```python\n{code.strip()}\n```")
    if last_error.strip():
        parts.append(f"The error I got:\n{last_error.strip()}")
    parts.append(f"My question: {question.strip() or 'I am stuck — give me a hint without the full answer.'}")
    return "\n\n".join(parts)


def _call_anthropic(s, user_msg: str) -> str:
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": s.tutor_api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": s.tutor_model or "claude-3-5-haiku-latest",
            "max_tokens": 600,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_msg}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"].strip()


def _call_openai(s, user_msg: str) -> str:
    resp = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {s.tutor_api_key}", "Content-Type": "application/json"},
        json={
            "model": s.tutor_model or "gpt-4o-mini",
            "max_tokens": 600,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _call_huggingface(s, user_msg: str) -> str:
    endpoint = s.tutor_hf_endpoint or (
        f"https://api-inference.huggingface.co/models/{s.tutor_model}" if s.tutor_model else ""
    )
    if not endpoint:
        raise ValueError("SQ_TUTOR_HF_ENDPOINT or SQ_TUTOR_MODEL is required for the huggingface provider")
    prompt = f"<s>[INST] {SYSTEM_PROMPT}\n\n{user_msg} [/INST]"
    resp = httpx.post(
        endpoint,
        headers={"Authorization": f"Bearer {s.tutor_api_key}", "Content-Type": "application/json"},
        json={"inputs": prompt, "parameters": {"max_new_tokens": 500, "return_full_text": False}},
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list) and data and "generated_text" in data[0]:
        return data[0]["generated_text"].strip()
    if isinstance(data, dict) and "generated_text" in data:
        return data["generated_text"].strip()
    return str(data)


def get_reply(
    *,
    brief: str,
    hints: list[str],
    concepts: list[str],
    code: str = "",
    question: str = "",
    last_error: str = "",
) -> tuple[str, str]:
    """Return ``(reply, provider_used)``. Falls back gracefully on any provider error."""
    s = get_settings()
    provider = (s.tutor_provider or "none").lower()
    if provider in ("none", "") or not s.tutor_api_key:
        return _fallback_reply(brief, hints, concepts, code, question, last_error), "rule-based"

    user_msg = _build_user_message(brief, code, question, last_error)
    try:
        if provider == "anthropic":
            return _call_anthropic(s, user_msg), "anthropic"
        if provider == "openai":
            return _call_openai(s, user_msg), "openai"
        if provider == "huggingface":
            return _call_huggingface(s, user_msg), "huggingface"
        return _fallback_reply(brief, hints, concepts, code, question, last_error), "rule-based"
    except Exception:  # noqa: BLE001 — never let the tutor break the lesson
        reply = _fallback_reply(brief, hints, concepts, code, question, last_error)
        return reply + "\n\n_(Live tutor unavailable right now — gave you an offline hint instead.)_", "rule-based"
