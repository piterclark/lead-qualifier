import re
import httpx


_IG_PATTERN = re.compile(
    r'instagram\.com/(?!p/|reel/|explore/|accounts/|stories/)([A-Za-z0-9_.]{2,30})',
    re.IGNORECASE,
)

_IRRELEVANT_IG = {
    "instagram", "accounts", "explore", "sharedfiles",
    "web", "www", "share", "create", "developer",
}


def _extract_instagram(html: str) -> str:
    matches = _IG_PATTERN.findall(html)
    for m in matches:
        username = m.strip("/").split("?")[0].split("/")[0]
        if username and username.lower() not in _IRRELEVANT_IG and len(username) >= 2:
            return username
    return ""


async def check_site(url: str) -> dict:
    """
    Verifica se o site está online e extrai o @instagram se existir na página.
    Retorna: {"alive": bool, "instagram": str}
    """
    result = {"alive": False, "instagram": ""}

    if not url:
        return result

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for attempt_url in [url, url.replace("https://", "http://")]:
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                resp = await client.get(attempt_url, headers=headers)
                if resp.status_code < 400:
                    result["alive"] = True
                    result["instagram"] = _extract_instagram(resp.text)
                    return result
        except Exception:
            continue

    return result
