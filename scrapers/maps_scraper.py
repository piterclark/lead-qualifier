import glob as _glob
import os
import re
import shutil
from typing import Callable

from playwright.async_api import async_playwright


def _find_chromium() -> str | None:
    # 1. PATH padrão (Docker, sistemas convencionais)
    for name in ("chromium", "chromium-browser", "google-chrome-stable", "google-chrome"):
        path = shutil.which(name)
        if path:
            return path
    # 2. Nix store (nixpacks no Railway — binaries nem sempre ficam no PATH em runtime)
    for pattern in ("/nix/store/*/bin/chromium", "/nix/store/*/bin/chromium-browser"):
        matches = sorted(_glob.glob(pattern))
        if matches:
            return matches[-1]
    return None


_SYSTEM_CHROMIUM = _find_chromium()

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-setuid-sandbox",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


async def scrape_maps(search_term: str, max_results: int, on_result: Callable) -> None:
    import random
    async with async_playwright() as p:
        launch_kwargs: dict = {"headless": True, "args": _LAUNCH_ARGS}
        if _SYSTEM_CHROMIUM:
            launch_kwargs["executable_path"] = _SYSTEM_CHROMIUM

        browser = await p.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            locale="pt-BR",
            user_agent=random.choice(_USER_AGENTS),
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True,
        )
        # Remove webdriver flag para evitar detecção de bot
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        encoded = search_term.replace(" ", "+")
        # domcontentloaded em vez de networkidle — Google Maps nunca atinge networkidle
        await page.goto(
            f"https://www.google.com/maps/search/{encoded}",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        # Aguarda o feed lateral aparecer (confirma que a busca carregou)
        try:
            await page.wait_for_selector('div[role="feed"], a[href*="/maps/place/"]', timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        sidebar_sel = 'div[role="feed"]'
        collected = 0
        last_count = 0
        stall_attempts = 0

        while collected < max_results:
            items = await page.query_selector_all('a[href*="/maps/place/"]')
            unique_hrefs = list(dict.fromkeys([await el.get_attribute("href") for el in items if await el.get_attribute("href")]))

            if len(unique_hrefs) <= last_count:
                stall_attempts += 1
                if stall_attempts >= 5:
                    break
            else:
                stall_attempts = 0
                last_count = len(unique_hrefs)

            for href in unique_hrefs[collected:]:
                if collected >= max_results:
                    break
                try:
                    lead = await _extract_from_href(page, href, context)
                    if lead:
                        await on_result(lead)
                        collected += 1
                except Exception:
                    pass

            try:
                feed = page.locator(sidebar_sel)
                await feed.evaluate("el => el.scrollBy(0, 800)")
                await page.wait_for_timeout(1500)
            except Exception:
                break

        await browser.close()


async def _extract_from_href(_page, href: str, context) -> dict | None:
    detail_page = await context.new_page()
    try:
        await detail_page.goto(href, wait_until="domcontentloaded", timeout=15000)
        await detail_page.wait_for_timeout(2000)

        name = await _text(detail_page, 'h1[class*="DUwDvf"]') or \
               await _text(detail_page, 'h1') or ""

        phone = ""
        phone_btn = detail_page.locator('button[data-item-id*="phone"]')
        if await phone_btn.count():
            phone = await phone_btn.first.get_attribute("data-item-id") or ""
            phone = phone.replace("phone:tel:", "").strip()

        website = ""
        site_btn = detail_page.locator('a[data-item-id="authority"]')
        if await site_btn.count():
            website = await site_btn.first.get_attribute("href") or ""

        address = await _text(detail_page, 'button[data-item-id*="address"]') or \
                  await _text(detail_page, '[data-item-id*="laddress"]') or ""

        rating = await _text(detail_page, 'div[jsaction*="rating"] span[aria-hidden]') or \
                 await _text(detail_page, 'span.ceNzKf') or ""

        instagram = ""
        page_content = await detail_page.content()
        ig_matches = re.findall(r'instagram\.com/([A-Za-z0-9_.]+)', page_content)
        if ig_matches:
            instagram = ig_matches[0].split("?")[0].split("/")[0]

        if not instagram:
            social_links = detail_page.locator('a[href*="instagram.com"]')
            if await social_links.count():
                ig_href = await social_links.first.get_attribute("href") or ""
                m = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', ig_href)
                if m:
                    instagram = m.group(1).split("?")[0]

        if not name:
            return None

        return {
            "name": name.strip(),
            "phone": _clean_phone(phone),
            "website": website.strip(),
            "instagram": instagram.strip().lstrip("@"),
            "address": address.strip(),
            "rating": rating.strip(),
            "ig_followers": 0,
            "ig_bio": "",
            "ig_url": "",
        }
    except Exception:
        return None
    finally:
        await detail_page.close()


async def _text(page, selector: str) -> str:
    try:
        el = page.locator(selector).first
        if await el.count():
            return (await el.inner_text()).strip()
    except Exception:
        pass
    return ""


def _clean_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) >= 10:
        return digits
    return raw
