# llm_summary.py
# T3 - send top-15 keywords to an LLM, return a grounded one-paragraph summary
# with a length cap (<=80 words) and a keyword-only fallback if the API fails.
import os
import json
import urllib.request
import urllib.error

PROMPT_TEMPLATE = (
    "You are a news desk editor. Given the following top keywords from the last hour "
    "of world-news headlines (with counts), write a SINGLE paragraph of at most 80 "
    "words that names AT LEAST three distinct storylines you can infer from the "
    "keywords. Ground every claim in the keywords - do not invent specific people, "
    "places, or numbers that are not implied by them. No bullets, no headings, no "
    "preamble; just the paragraph.\n\n"
    "Keywords (word: count):\n{keywords}\n"
)


def _format_keywords(pairs) -> str:
    return "\n".join(f"- {w}: {c}" for w, c in pairs)


def _truncate_words(text: str, max_words: int = 80) -> str:
    parts = text.strip().split()
    if len(parts) <= max_words:
        return text.strip()
    return " ".join(parts[:max_words]).rstrip(",.;:") + "."


def _fallback(pairs) -> str:
    if not pairs:
        return "No keywords available yet - the stream is still warming up."
    top = ", ".join(w for w, _ in pairs[:8])
    return (
        f"Live keyword snapshot only (LLM unavailable). The current news pulse is "
        f"dominated by: {top}. Three storyline candidates: (1) coverage clustered "
        f"around the leading term, (2) a secondary thread tying the next two terms, "
        f"and (3) a long-tail mix across the remaining keywords."
    )


def _call_anthropic(prompt: str, key: str) -> str:
    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        payload = json.loads(r.read().decode("utf-8"))
    # Anthropic returns content as a list of blocks
    return "".join(b.get("text", "") for b in payload.get("content", []))


def _call_openai(prompt: str, key: str) -> str:
    body = json.dumps({
        "model": "gpt-4o-mini",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        payload = json.loads(r.read().decode("utf-8"))
    return payload["choices"][0]["message"]["content"]


def summarise(keyword_pairs):
    """keyword_pairs: list[(word, count)] already sorted desc, length up to 15."""
    pairs = list(keyword_pairs)[:15]
    if not pairs:
        return _fallback(pairs)

    prompt = PROMPT_TEMPLATE.format(keywords=_format_keywords(pairs))

    a_key = os.getenv("ANTHROPIC_API_KEY")
    o_key = os.getenv("OPENAI_API_KEY")

    try:
        if a_key:
            return _truncate_words(_call_anthropic(prompt, a_key))
        if o_key:
            return _truncate_words(_call_openai(prompt, o_key))
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError, TimeoutError) as ex:
        print(f"[llm_summary] API call failed ({ex!r}); falling back to keyword-only summary")

    return _fallback(pairs)


if __name__ == "__main__":
    demo = [("gaza", 12), ("election", 9), ("ai", 7), ("market", 6), ("ukraine", 5)]
    print(summarise(demo))
