import httpx


async def check_site(url: str) -> bool:
    """Returns True if the URL resolves to a live website."""
    if not url:
        return False

    # Normalize URL
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            return resp.status_code < 400
    except Exception:
        # Try http fallback
        try:
            fallback = url.replace("https://", "http://")
            async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
                resp = await client.get(fallback, headers={"User-Agent": "Mozilla/5.0"})
                return resp.status_code < 400
        except Exception:
            return False
