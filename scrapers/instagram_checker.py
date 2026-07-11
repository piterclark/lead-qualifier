import re
import instaloader


_AUTHORITY_KEYWORDS = [
    "especialista", "lista de espera", "referência", "referencia",
    "agenda fechada", "agenda lotada", "mais de", "atendo",
    "dra.", "dr.", "pós-graduada", "pós graduada", "mestrado",
    "doutorado", "supervisão", "supervisao", "formação", "formacao",
    "crp", "coorientadora", "professora", "psicóloga clínica", "psicologa clinica",
]

_LINKTREE_KEYWORDS = [
    "linktree", "linktr.ee", "beacons.ai", "bio.site", "campsite.bio",
    "allmylinks", "later.com/linksinbio", "flowpage", "lnk.bio",
]

_LOADER = None


def _get_loader():
    global _LOADER
    if _LOADER is None:
        _LOADER = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            quiet=True,
        )
    return _LOADER


def check_instagram(username: str) -> dict:
    """
    Sync function (run in executor).
    Returns dict with:
      - found (bool)
      - followers (int)
      - bio (str)
      - external_url (str)
      - strong_positioning (bool) → DISCARD
      - needs_review (bool)      → manual review flag
    """
    result = {
        "found": False,
        "followers": 0,
        "bio": "",
        "external_url": "",
        "strong_positioning": False,
        "needs_review": False,
    }

    if not username:
        return result

    username = username.strip().lstrip("@").split("/")[-1].split("?")[0]
    if not username:
        return result

    try:
        loader = _get_loader()
        profile = instaloader.Profile.from_username(loader.context, username)

        followers = profile.followers
        bio = profile.biography or ""
        external_url = profile.external_url or ""

        result["found"] = True
        result["followers"] = followers
        result["bio"] = bio
        result["external_url"] = external_url

        bio_lower = bio.lower()
        has_authority_keyword = any(kw in bio_lower for kw in _AUTHORITY_KEYWORDS)
        has_linktree = any(kw in (external_url or "").lower() for kw in _LINKTREE_KEYWORDS)
        has_external_link = bool(external_url and external_url.strip())

        # Strong positioning = already well-positioned → DISCARD
        if followers >= 1000:
            result["strong_positioning"] = True
        elif followers >= 500 and (has_authority_keyword or has_linktree):
            result["strong_positioning"] = True
        elif has_authority_keyword and has_linktree:
            result["strong_positioning"] = True
        elif has_authority_keyword and has_external_link and followers >= 300:
            result["strong_positioning"] = True

        # Needs review = borderline cases
        if not result["strong_positioning"]:
            if 300 <= followers < 1000:
                result["needs_review"] = True
            elif followers >= 200 and has_authority_keyword:
                result["needs_review"] = True
            elif has_linktree and followers >= 100:
                result["needs_review"] = True

    except instaloader.exceptions.ProfileNotExistsException:
        result["found"] = False
    except instaloader.exceptions.LoginRequiredException:
        # Private account — can't analyze, mark as needs_review
        result["found"] = True
        result["needs_review"] = True
        result["bio"] = "[conta privada]"
    except Exception:
        # Network error or rate limit — treat as needs_review to be safe
        result["found"] = False
        result["needs_review"] = True

    return result
