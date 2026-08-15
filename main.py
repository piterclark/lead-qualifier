import os
import re
import asyncio
import csv
import io
import json
from datetime import datetime
from pathlib import Path

_PB_PATH = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/app/playwright-browsers")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = _PB_PATH

from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from scrapers.maps_scraper import scrape_maps
from scrapers.site_checker import check_site
from scrapers.instagram_checker import check_instagram

app = FastAPI(title="Lead Qualifier — Esteticistas")

# ── Supabase ──────────────────────────────────────────────────────────────────
_supabase_client = None

def _get_sb():
    global _supabase_client
    if _supabase_client is None:
        _url = os.environ.get("SUPABASE_URL", "")
        _key = os.environ.get("SUPABASE_KEY", "")
        if _url and _key:
            try:
                from supabase import create_client
                _supabase_client = create_client(_url, _key)
            except Exception:
                pass
    return _supabase_client

async def _sb(fn):
    """Run a sync supabase call in a thread executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, fn)

# ── Browser download state ────────────────────────────────────────────────────
_browser_state: dict = {"status": "unknown", "error": None}


def _browser_ready() -> bool:
    import glob as _g
    pb = Path(_PB_PATH)
    return pb.exists() and bool(_g.glob(f"{_PB_PATH}/**/chrome*", recursive=True))


async def _download_browser_bg():
    """Downloads playwright chromium on first startup if not already present."""
    if _browser_ready():
        _browser_state["status"] = "ready"
        return
    _browser_state["status"] = "downloading"
    try:
        pb = Path(_PB_PATH)
        pb.mkdir(parents=True, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            "python", "-m", "playwright", "install", "--with-deps", "chromium",
            env={**os.environ},
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            _browser_state["status"] = "error"
            _browser_state["error"] = (stderr or b"").decode()[-500:]
        else:
            _browser_state["status"] = "ready" if _browser_ready() else "error"
    except Exception as exc:
        _browser_state["status"] = "error"
        _browser_state["error"] = str(exc)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_download_browser_bg())
    # Load known_leads from Supabase
    sb = _get_sb()
    if sb:
        try:
            res = await _sb(lambda: sb.table("known_leads").select("*").execute())
            for row in (res.data or []):
                known_leads[row["key"]] = {
                    "name": row.get("name", ""),
                    "status": row.get("status", ""),
                    "cidade": row.get("cidade", ""),
                    "scan_time": row.get("scan_time", ""),
                    "scan_id": row.get("scan_id", ""),
                }
        except Exception:
            pass

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_DIR = DATA_DIR / "history"
HISTORY_DIR.mkdir(exist_ok=True)
KNOWN_LEADS_FILE = DATA_DIR / "known_leads.json"

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

LEADS_FILE = DATA_DIR / "leads.json"
STATS_FILE = DATA_DIR / "stats.json"

# Estado global — persiste entre reconexões SSE
scan_state = {
    "running": False,
    "stop_requested": False,
    "leads": [],
    "stats": {"qualificados": 0, "descartados_site": 0, "descartados_ig": 0, "descartados_sem_contato": 0, "revisar": 0, "total": 0},
    "scan_time": None,       # "dd/mm/yyyy às HH:MM" — exibido no drawer de cada step
    "_task": None,           # asyncio.Task — persiste mesmo sem clientes SSE
    "_subscribers": [],      # asyncio.Queue por cliente SSE conectado
    "_log_buffer": [],       # últimos 300 logs para reconexão rápida
    "_cidade": "",
    "_max_results": 0,
    "_scan_id": "",
}

# Carregar leads salvos em sessão anterior (se existirem)
if LEADS_FILE.exists():
    try:
        _saved = json.loads(LEADS_FILE.read_text(encoding="utf-8"))
        scan_state["leads"] = _saved
    except Exception:
        pass
if STATS_FILE.exists():
    try:
        _saved_stats = json.loads(STATS_FILE.read_text(encoding="utf-8"))
        scan_state["stats"] = _saved_stats
    except Exception:
        pass

known_leads: dict = {}
if KNOWN_LEADS_FILE.exists():
    try:
        known_leads = json.loads(KNOWN_LEADS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass


def _save_to_disk():
    try:
        LEADS_FILE.write_text(json.dumps(scan_state["leads"], ensure_ascii=False, indent=2), encoding="utf-8")
        STATS_FILE.write_text(json.dumps(scan_state["stats"], ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _save_known_leads():
    try:
        KNOWN_LEADS_FILE.write_text(json.dumps(known_leads, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


async def _save_to_history():
    """Persiste a varredura concluída no Supabase e localmente."""
    if not scan_state["leads"]:
        return
    scan_id = scan_state.get("_scan_id") or f"SCAN-{int(__import__('time').time())}"
    entry = {
        "id": scan_id,
        "scan_id": scan_id,
        "trashed": False,
        "scan_time": scan_state.get("scan_time", "—"),
        "cidade": scan_state.get("_cidade", ""),
        "max_results": scan_state.get("_max_results", 0),
        "stats": dict(scan_state["stats"]),
        "leads": list(scan_state["leads"]),
    }
    # Save to Supabase
    sb = _get_sb()
    if sb:
        try:
            db_entry = {k: v for k, v in entry.items() if k != "id"}
            _e = db_entry
            await _sb(lambda: sb.table("scans").upsert(_e).execute())
        except Exception:
            pass
    # Save to local file (within-session SSE recovery)
    try:
        (HISTORY_DIR / f"{scan_id}.json").write_text(
            json.dumps(entry, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass


async def _push_known_leads_to_db():
    """Upserts known_leads from current scan to Supabase."""
    sb = _get_sb()
    if not sb:
        return
    current_scan_id = scan_state.get("_scan_id", "")
    rows = [
        {"key": k, "name": v.get("name", ""), "status": v.get("status", ""),
         "cidade": v.get("cidade", ""), "scan_time": v.get("scan_time", ""),
         "scan_id": v.get("scan_id", "")}
        for k, v in known_leads.items()
        if v.get("scan_id") == current_scan_id
    ]
    if rows:
        try:
            _rows = rows
            await _sb(lambda: sb.table("known_leads").upsert(_rows).execute())
        except Exception:
            pass


def _broadcast(event: dict):
    """Envia evento para todos os clientes SSE conectados."""
    dead = []
    for q in scan_state["_subscribers"]:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            scan_state["_subscribers"].remove(q)
        except ValueError:
            pass

    if event.get("type") == "log":
        scan_state["_log_buffer"].append(event)
        if len(scan_state["_log_buffer"]) > 300:
            scan_state["_log_buffer"].pop(0)


async def _run_scan(cidade: str, max_results: int):
    """
    Tarefa de fundo — roda independente de conexões SSE.
    Sobrevive se o browser fechar; retoma ao reconectar.
    """
    try:
        import time as _t0
        scan_state["running"] = True
        scan_state["stop_requested"] = False
        scan_state["leads"] = []
        scan_state["_log_buffer"] = []
        scan_state["_cidade"] = cidade
        scan_state["_max_results"] = max_results
        scan_state["_scan_id"] = f"SCAN-{int(_t0.time())}"
        scan_state["scan_time"] = datetime.now().strftime("%d/%m/%Y às %H:%M")
        scan_state["stats"] = {
            "qualificados": 0, "descartados_site": 0,
            "descartados_ig": 0, "revisar": 0, "total": 0
        }

        def log(msg: str):
            _broadcast({"type": "log", "msg": msg})

        def push_lead(lead: dict):
            phone = re.sub(r'\D', '', lead.get("phone", ""))
            k = phone if len(phone) >= 8 else lead.get("name", "").lower().strip()
            if k and k in known_leads:
                prev = known_leads[k]
                lead["previous_result"] = {
                    "status": prev.get("status"),
                    "cidade": prev.get("cidade"),
                    "scan_time": prev.get("scan_time"),
                }
            scan_state["leads"].append(lead)
            scan_state["stats"]["total"] += 1
            _broadcast({"type": "lead", "lead": lead})
            _broadcast({"type": "stats", "stats": dict(scan_state["stats"])})
            if k:
                known_leads[k] = {
                    "name": lead.get("name", ""),
                    "status": lead.get("status", ""),
                    "cidade": scan_state.get("_cidade", ""),
                    "scan_time": scan_state.get("scan_time", ""),
                    "scan_id": scan_state.get("_scan_id", ""),
                }
            if len(scan_state["leads"]) % 10 == 0:
                _save_to_disk()
                _save_known_leads()

        log(f"◈ SISTEMA INICIADO — buscando esteticistas em {cidade}")
        log("◈ Abrindo Google Maps (modo stealth)...")

        results_queue: asyncio.Queue = asyncio.Queue()
        seen_phones: set = set()
        _DONE = object()

        SEARCH_TERMS = [
            "esteticista",
            "clínica de estética",
            "studio de estética",
            "salão de estética",
        ]

        def _dedup_key(lead: dict) -> str:
            import re as _re
            phone = _re.sub(r'\D', '', lead.get("phone", ""))
            return phone if len(phone) >= 8 else lead.get("name", "").lower().strip()

        async def maps_callback(lead: dict):
            key = _dedup_key(lead)
            if key and key in seen_phones:
                return
            if key:
                seen_phones.add(key)
            await results_queue.put(lead)

        async def producer():
            n = len(SEARCH_TERMS)
            for i, term in enumerate(SEARCH_TERMS, 1):
                if scan_state["stop_requested"] or len(scan_state["leads"]) >= max_results:
                    break
                full_term = f"{term} {cidade}"
                log(f"◈ [{i}/{n}] Buscando: «{full_term}»...")
                try:
                    await scrape_maps(
                        search_term=full_term,
                        max_results=max_results,
                        on_result=maps_callback,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log(f"◈ Aviso — erro no termo «{term}»: {type(e).__name__}: {e}")
                log(f"◈ Termo {i}/{n} concluído — {len(seen_phones)} locais únicos encontrados")
            await results_queue.put(_DONE)

        producer_task = asyncio.create_task(producer())

        processed = 0
        while True:
            if scan_state["stop_requested"]:
                producer_task.cancel()
                log("◈ Varredura interrompida pelo operador.")
                break

            try:
                lead = await asyncio.wait_for(results_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            if lead is _DONE:
                break

            if len(scan_state["leads"]) >= max_results:
                producer_task.cancel()
                break

            processed += 1
            log(f"  → [{processed}] Analisando: {lead.get('name', '?')}")

            # ── Detectar site que é na verdade uma rede social ──────────────
            _website = lead.get("website", "")
            _is_social = False
            if _website:
                _wl = _website.lower()
                if "instagram.com" in _wl:
                    _m = re.search(r'instagram\.com/([^/?#\s]+)', _website, re.IGNORECASE)
                    if _m:
                        _ig_slug = _m.group(1).strip('/').strip()
                        _blocked = {'p', 'reel', 'reels', 'explore', 'stories', 'tv', 'accounts', 'ar', 'web'}
                        if _ig_slug and _ig_slug.lower() not in _blocked:
                            lead["instagram"] = lead.get("instagram") or _ig_slug
                    _is_social = True
                elif any(s in _wl for s in ("facebook.com", "fb.com", "linktr.ee", "wa.me", "whatsapp.com", "tiktok.com")):
                    _is_social = True

            # ── Etapa 2: site real ───────────────────────────────────────────
            if lead.get("website") and not _is_social:
                site_result = await check_site(lead["website"])
                lead["site_alive"] = site_result["alive"]
                if site_result["alive"]:
                    lead["status"] = "DESCARTADO"
                    lead["motivo"] = f"Possui site ({lead['website']})"
                    scan_state["stats"]["descartados_site"] += 1
                    push_lead(lead)
                    continue
                if site_result["instagram"] and not lead.get("instagram"):
                    lead["instagram"] = site_result["instagram"]

            # ── Etapa 3: Instagram ───────────────────────────────────────────
            if not lead.get("instagram") and lead.get("website") and not _is_social:
                site_result = await check_site(lead["website"])
                if site_result["instagram"]:
                    lead["instagram"] = site_result["instagram"]

            ig_username = lead.get("instagram", "").strip().lstrip("@")
            if ig_username:
                log(f"    → @{ig_username} — verificando Instagram...")
                loop = asyncio.get_event_loop()
                ig_result = await loop.run_in_executor(None, check_instagram, ig_username)
                lead["ig_followers"] = ig_result.get("followers", 0)
                lead["ig_bio"] = ig_result.get("bio", "")[:120]
                lead["ig_url"] = f"https://instagram.com/{ig_username}"

                if not lead.get("phone") and ig_result.get("bio"):
                    _bio = ig_result["bio"]
                    _pm = re.search(r'(\+?[\d][\d\s\(\)\-]{6,}[\d])', _bio)
                    if _pm:
                        _digits = re.sub(r'\D', '', _pm.group(1))
                        if 8 <= len(_digits) <= 15:
                            lead["phone"] = _pm.group(1).strip()
                            lead["phone_source"] = "bio_instagram"
                            log(f"    → 📱 Telefone extraído da bio do IG: {lead['phone']}")

                if ig_result.get("strong_positioning"):
                    lead["status"] = "DESCARTADO"
                    lead["motivo"] = f"Posicionamento forte (IG: {ig_result.get('followers', 0)} seg.)"
                    scan_state["stats"]["descartados_ig"] += 1
                    push_lead(lead)
                    continue

                if ig_result.get("needs_review"):
                    lead["status"] = "REVISAR"
                    lead["motivo"] = "Instagram borderline — revisar feed"
                    scan_state["stats"]["revisar"] += 1
                    push_lead(lead)
                    continue

            # ── Regra mínima de contato ──────────────────────────────────────
            _has_phone = bool(re.sub(r'\D', '', lead.get("phone", "")))
            _has_contact = _has_phone or bool(ig_username)
            if not _has_contact:
                lead["status"] = "DESCARTADO"
                lead["motivo"] = "Sem contato (sem telefone e sem Instagram)"
                scan_state["stats"]["descartados_sem_contato"] += 1
                push_lead(lead)
                continue

            # ── Qualificado ──────────────────────────────────────────────────
            _contato_parts = []
            if not lead.get("website") or _is_social:
                _contato_parts.append("sem site")
            if ig_username:
                _contato_parts.append(f"IG fraco (@{ig_username})")
            elif not lead.get("phone"):
                _contato_parts.append("sem Instagram")
            if lead.get("phone_source") == "bio_instagram":
                _contato_parts.append("tel. via bio IG")
            lead["status"] = "QUALIFICADO"
            lead["motivo"] = " — ".join(_contato_parts) if _contato_parts else "sem site e sem Instagram"
            scan_state["stats"]["qualificados"] += 1
            push_lead(lead)

        try:
            await producer_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log(f"◈ ERRO NO PRODUTOR: {type(e).__name__}: {e}")

        _save_to_disk()
        _save_known_leads()
        await _save_to_history()
        await _push_known_leads_to_db()
        scan_state["running"] = False
        _broadcast({"type": "complete", "stats": dict(scan_state["stats"])})
        log(
            f"◈ VARREDURA CONCLUÍDA — "
            f"{scan_state['stats']['qualificados']} qualificados | "
            f"{scan_state['stats']['total']} analisados"
        )

    except Exception as e:
        scan_state["running"] = False
        _broadcast({"type": "log", "msg": f"◈ ERRO CRÍTICO: {e}"})
        _broadcast({"type": "complete", "stats": dict(scan_state["stats"])})
    finally:
        scan_state["_task"] = None


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/clear-panel")
async def clear_panel():
    """Limpa o painel atual sem apagar o histórico."""
    scan_state["leads"] = []
    scan_state["stats"] = {"qualificados": 0, "descartados_site": 0, "descartados_ig": 0, "descartados_sem_contato": 0, "revisar": 0, "total": 0}
    scan_state["scan_time"] = None
    scan_state["_cidade"] = ""
    scan_state["_log_buffer"] = []
    for f in [LEADS_FILE, STATS_FILE]:
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass
    return {"ok": True}


@app.get("/api/reset")
async def reset_all():
    """Zera painel atual e known_leads — histórico NUNCA é apagado."""
    scan_state["leads"] = []
    scan_state["stats"] = {"qualificados": 0, "descartados_site": 0, "descartados_ig": 0, "descartados_sem_contato": 0, "revisar": 0, "total": 0}
    scan_state["scan_time"] = None
    scan_state["_cidade"] = ""
    scan_state["_log_buffer"] = []
    known_leads.clear()
    # Clear from Supabase
    sb = _get_sb()
    if sb:
        try:
            await _sb(lambda: sb.table("known_leads").delete().neq("key", "").execute())
        except Exception:
            pass
    for f in [LEADS_FILE, STATS_FILE, KNOWN_LEADS_FILE]:
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass
    return {"ok": True, "msg": "Painel e known_leads zerados — histórico preservado."}


@app.post("/api/trash-scan/{scan_id}")
async def trash_scan(scan_id: str):
    """Move varredura para lixeira e remove seus leads do known_leads."""
    sb = _get_sb()

    if sb:
        try:
            _id = scan_id
            res = await _sb(lambda: sb.table("scans").select("*").eq("scan_id", _id).execute())
            if res.data:
                data = res.data[0]
                await _sb(lambda: sb.table("scans").update({"trashed": True}).eq("scan_id", _id).execute())
                await _sb(lambda: sb.table("known_leads").delete().eq("scan_id", _id).execute())
                removed = 0
                for lead in data.get("leads", []):
                    phone = re.sub(r'\D', '', lead.get("phone", ""))
                    k = phone if len(phone) >= 8 else lead.get("name", "").lower().strip()
                    if k and k in known_leads and known_leads[k].get("scan_id") == scan_id:
                        del known_leads[k]
                        removed += 1
                _save_known_leads()
                f = HISTORY_DIR / f"{scan_id}.json"
                if f.exists():
                    try:
                        local = json.loads(f.read_text(encoding="utf-8"))
                        local["trashed"] = True
                        f.write_text(json.dumps(local, ensure_ascii=False), encoding="utf-8")
                    except Exception:
                        pass
                return {"ok": True, "removed_from_known": removed}
        except Exception:
            pass

    # Fallback to local file
    f = HISTORY_DIR / f"{scan_id}.json"
    if not f.exists():
        raise HTTPException(status_code=404, detail="Varredura não encontrada")
    data = json.loads(f.read_text(encoding="utf-8"))
    data["trashed"] = True
    f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    removed = 0
    for lead in data.get("leads", []):
        phone = re.sub(r'\D', '', lead.get("phone", ""))
        k = phone if len(phone) >= 8 else lead.get("name", "").lower().strip()
        if k and k in known_leads and known_leads[k].get("scan_id") == scan_id:
            del known_leads[k]
            removed += 1
    _save_known_leads()
    return {"ok": True, "removed_from_known": removed}


@app.post("/api/restore-scan/{scan_id}")
async def restore_scan(scan_id: str):
    """Restaura varredura da lixeira e re-adiciona leads ao known_leads."""
    sb = _get_sb()

    if sb:
        try:
            _id = scan_id
            res = await _sb(lambda: sb.table("scans").select("*").eq("scan_id", _id).execute())
            if res.data:
                data = res.data[0]
                await _sb(lambda: sb.table("scans").update({"trashed": False}).eq("scan_id", _id).execute())
                added = 0
                rows_to_upsert = []
                for lead in data.get("leads", []):
                    phone = re.sub(r'\D', '', lead.get("phone", ""))
                    k = phone if len(phone) >= 8 else lead.get("name", "").lower().strip()
                    if k and k not in known_leads:
                        known_leads[k] = {
                            "name": lead.get("name", ""),
                            "status": lead.get("status", ""),
                            "cidade": data.get("cidade", ""),
                            "scan_time": data.get("scan_time", ""),
                            "scan_id": scan_id,
                        }
                        rows_to_upsert.append({"key": k, **known_leads[k]})
                        added += 1
                if rows_to_upsert:
                    _rows = rows_to_upsert
                    await _sb(lambda: sb.table("known_leads").upsert(_rows).execute())
                _save_known_leads()
                f = HISTORY_DIR / f"{scan_id}.json"
                if f.exists():
                    try:
                        local = json.loads(f.read_text(encoding="utf-8"))
                        local["trashed"] = False
                        f.write_text(json.dumps(local, ensure_ascii=False), encoding="utf-8")
                    except Exception:
                        pass
                return {"ok": True, "added_to_known": added}
        except Exception:
            pass

    # Fallback to local file
    f = HISTORY_DIR / f"{scan_id}.json"
    if not f.exists():
        raise HTTPException(status_code=404, detail="Varredura não encontrada")
    data = json.loads(f.read_text(encoding="utf-8"))
    data["trashed"] = False
    f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    added = 0
    for lead in data.get("leads", []):
        phone = re.sub(r'\D', '', lead.get("phone", ""))
        k = phone if len(phone) >= 8 else lead.get("name", "").lower().strip()
        if k and k not in known_leads:
            known_leads[k] = {
                "name": lead.get("name", ""),
                "status": lead.get("status", ""),
                "cidade": data.get("cidade", ""),
                "scan_time": data.get("scan_time", ""),
                "scan_id": scan_id,
            }
            added += 1
    _save_known_leads()
    return {"ok": True, "added_to_known": added}


@app.get("/api/version")
async def version():
    import subprocess
    try:
        commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        commit = "unknown"
    return {"commit": commit, "title": app.title}


@app.get("/api/health")
async def health():
    import glob as _glob
    from scrapers.maps_scraper import _find_chromium, _SYSTEM_CHROMIUM

    pb_exists = Path(_PB_PATH).exists()
    pb_top = sorted(str(p.name) for p in Path(_PB_PATH).iterdir()) if pb_exists else []
    chrome_bins = _glob.glob(f"{_PB_PATH}/**/chrome*", recursive=True) if pb_exists else []

    ok = bool(chrome_bins) or bool(_SYSTEM_CHROMIUM or _find_chromium())
    return {
        "ok": ok,
        "browser_status": _browser_state["status"],
        "browser_error": _browser_state["error"],
        "PLAYWRIGHT_BROWSERS_PATH": _PB_PATH,
        "pb_dir_exists": pb_exists,
        "pb_top_level": pb_top,
        "playwright_bins": chrome_bins[:3],
        "app_dir": sorted(str(p.name) for p in Path("/app").iterdir()) if Path("/app").exists() else [],
        "supabase_connected": _get_sb() is not None,
    }


@app.get("/api/test-browser")
async def test_browser():
    from playwright.async_api import async_playwright
    import traceback
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-setuid-sandbox", "--disable-gpu"]
            )
            version = browser.version
            await browser.close()
        return {"ok": True, "browser_version": version}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "traceback": traceback.format_exc()[-2000:]}


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/status")
async def status():
    return {
        "running": scan_state["running"],
        "cidade": scan_state["_cidade"],
        "stats": scan_state["stats"],
        "lead_count": len(scan_state["leads"]),
        "stop_requested": scan_state["stop_requested"],
    }


@app.get("/api/leads")
async def get_leads(status_filter: str = "ALL"):
    leads = scan_state["leads"]
    if status_filter != "ALL":
        leads = [l for l in leads if l.get("status") == status_filter]
    return {
        "leads": leads,
        "stats": scan_state["stats"],
        "scan_time": scan_state.get("scan_time"),
        "cidade": scan_state.get("_cidade", ""),
        "running": scan_state["running"],
        "total": len(leads),
    }


@app.get("/api/stop")
async def stop_scan():
    scan_state["stop_requested"] = True
    return {"ok": True}


@app.get("/api/scan")
async def scan(
    cidade: str = Query(...),
    max_results: int = Query(default=200, le=2500),
):
    task_done = scan_state["_task"] is None or scan_state["_task"].done()
    if not scan_state["running"] and task_done:
        scan_state["_task"] = asyncio.create_task(_run_scan(cidade, max_results))

    q: asyncio.Queue = asyncio.Queue(maxsize=2000)
    scan_state["_subscribers"].append(q)

    for event in scan_state["_log_buffer"][-100:]:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            break

    for lead in scan_state["leads"]:
        try:
            q.put_nowait({"type": "lead", "lead": lead})
        except asyncio.QueueFull:
            break
    try:
        q.put_nowait({"type": "stats", "stats": dict(scan_state["stats"])})
    except asyncio.QueueFull:
        pass

    async def event_stream():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=25.0)
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    if event.get("type") == "complete":
                        break
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    if not scan_state["running"]:
                        break
        except GeneratorExit:
            pass
        finally:
            try:
                scan_state["_subscribers"].remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    async def event_stream():
        scan_state["running"] = True
        scan_state["stop_requested"] = False
        scan_state["leads"] = []
        scan_state["stats"] = {
            "qualificados": 0, "descartados_site": 0,
            "descartados_ig": 0, "revisar": 0, "total": 0
        }

        def send(data: dict) -> str:
            return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

        content = await file.read()
        text = content.decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        yield send({"type": "log", "msg": f"◈ CSV carregado — {len(rows)} contatos detectados"})

        for i, row in enumerate(rows):
            if scan_state["stop_requested"]:
                yield send({"type": "log", "msg": "◈ Processamento interrompido."})
                break

            lead = {
                "name": row.get("Nome") or row.get("name") or row.get("NOME") or "",
                "phone": row.get("Telefone") or row.get("phone") or row.get("TELEFONE") or "",
                "website": row.get("Site") or row.get("website") or row.get("SITE") or "",
                "instagram": row.get("Instagram") or row.get("instagram") or row.get("INSTAGRAM") or "",
                "address": row.get("Endereço") or row.get("address") or row.get("ENDERECO") or "",
                "rating": row.get("Avaliação") or row.get("rating") or "",
                "ig_followers": 0, "ig_bio": "", "ig_url": "",
            }

            yield send({"type": "log", "msg": f"  → [{i+1}/{len(rows)}] {lead['name']}"})

            if lead["website"]:
                has_site = await check_site(lead["website"])
                if has_site:
                    lead["status"] = "DESCARTADO"
                    lead["motivo"] = "Possui site"
                    scan_state["stats"]["descartados_site"] += 1
                    scan_state["leads"].append(lead)
                    scan_state["stats"]["total"] += 1
                    yield send({"type": "lead", "lead": lead})
                    yield send({"type": "stats", "stats": scan_state["stats"]})
                    continue

            ig_username = lead["instagram"].strip().lstrip("@").split("/")[-1].split("?")[0]
            if ig_username:
                yield send({"type": "log", "msg": f"    → @{ig_username} — verificando Instagram..."})
                loop = asyncio.get_event_loop()
                ig_result = await loop.run_in_executor(None, check_instagram, ig_username)
                lead["ig_followers"] = ig_result.get("followers", 0)
                lead["ig_bio"] = ig_result.get("bio", "")[:120]
                lead["ig_url"] = f"https://instagram.com/{ig_username}"

                if ig_result.get("strong_positioning"):
                    lead["status"] = "DESCARTADO"
                    lead["motivo"] = f"Posicionamento forte ({ig_result.get('followers', 0)} seg.)"
                    scan_state["stats"]["descartados_ig"] += 1
                    scan_state["leads"].append(lead)
                    scan_state["stats"]["total"] += 1
                    yield send({"type": "lead", "lead": lead})
                    yield send({"type": "stats", "stats": scan_state["stats"]})
                    continue

                if ig_result.get("needs_review"):
                    lead["status"] = "REVISAR"
                    lead["motivo"] = "Instagram borderline — revisar feed"
                    scan_state["stats"]["revisar"] += 1
                    scan_state["leads"].append(lead)
                    scan_state["stats"]["total"] += 1
                    yield send({"type": "lead", "lead": lead})
                    yield send({"type": "stats", "stats": scan_state["stats"]})
                    continue

            lead["status"] = "QUALIFICADO"
            lead["motivo"] = (
                "Sem site" +
                (f" — IG fraco (@{ig_username})" if ig_username else " e sem Instagram")
            )
            scan_state["stats"]["qualificados"] += 1
            scan_state["leads"].append(lead)
            scan_state["stats"]["total"] += 1
            yield send({"type": "lead", "lead": lead})
            yield send({"type": "stats", "stats": scan_state["stats"]})

        _save_to_disk()
        scan_state["running"] = False
        yield send({"type": "complete", "stats": scan_state["stats"]})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/step-data/{step}")
async def step_data(step: str):
    leads = scan_state["leads"]
    scan_time = scan_state.get("scan_time") or "—"

    if step == "maps":
        data = [
            {"name": l.get("name", ""), "phone": l.get("phone", ""),
             "address": l.get("address", ""), "rating": l.get("rating", "")}
            for l in leads
        ]
    elif step == "site":
        data = [
            {"name": l.get("name", ""), "website": l.get("website", ""),
             "site_alive": l.get("site_alive"), "instagram": l.get("instagram", "")}
            for l in leads if l.get("website")
        ]
    elif step == "instagram":
        data = [
            {"name": l.get("name", ""), "instagram": l.get("instagram", ""),
             "ig_followers": l.get("ig_followers", 0), "ig_bio": l.get("ig_bio", "")}
            for l in leads if l.get("instagram")
        ]
    elif step == "qualificado":
        data = [
            {"name": l.get("name", ""), "phone": l.get("phone", ""),
             "status": l.get("status", ""), "motivo": l.get("motivo", "")}
            for l in leads
        ]
    else:
        data = []

    cidade = scan_state.get("_cidade", "")
    return {"step": step, "scan_time": scan_time, "cidade": cidade, "total": len(data), "data": data}


def _wa_link(phone: str) -> str:
    """Gera link wa.me a partir de um número de telefone."""
    if not phone:
        return ""
    d = re.sub(r'\D', '', phone)
    if not d:
        return ""
    if d.startswith('55') and len(d) >= 12:
        n = d
    elif d.startswith('0'):
        n = '55' + d[1:]
    else:
        n = '55' + d
    return f"https://wa.me/{n}"


@app.get("/api/export")
async def export_csv(filter: str = "QUALIFICADO"):
    leads = scan_state["leads"]
    if filter != "ALL":
        leads = [l for l in leads if l.get("status") == filter]

    fieldnames = ["name", "phone", "whatsapp_link", "address", "website", "instagram",
                  "ig_url", "ig_followers", "ig_bio", "status", "motivo", "rating"]

    def _clean(row: dict) -> dict:
        cleaned = {}
        for k, v in row.items():
            if isinstance(v, str):
                v = v.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
            cleaned[k] = v
        cleaned["whatsapp_link"] = _wa_link(cleaned.get("phone", ""))
        return cleaned

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(_clean(l) for l in leads)

    content = output.getvalue().encode("utf-8-sig")

    from fastapi.responses import Response
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{filter.lower()}.csv"},
    )


@app.get("/api/history")
async def list_history():
    """Lista varreduras salvas separando ativas de lixeira."""
    sb = _get_sb()

    if sb:
        try:
            res = await _sb(lambda: sb.table("scans").select("scan_id,scan_time,cidade,max_results,stats,leads,trashed").order("created_at", desc=True).limit(200).execute())
            active, trashed = [], []
            for row in (res.data or []):
                entry = {
                    "id": row["scan_id"],
                    "scan_time": row.get("scan_time", "—"),
                    "cidade": row.get("cidade", "—"),
                    "max_results": row.get("max_results", 0),
                    "stats": row.get("stats", {}),
                    "total": len(row.get("leads", [])),
                    "trashed": row.get("trashed", False),
                }
                (trashed if row.get("trashed") else active).append(entry)
            return {"history": active, "trashed": trashed, "count": len(active)}
        except Exception:
            pass

    # Fallback to local files
    active, trashed = [], []
    for f in sorted(HISTORY_DIR.glob("SCAN-*.json"), reverse=True)[:200]:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            entry = {
                "id": data["id"],
                "scan_time": data.get("scan_time", "—"),
                "cidade": data.get("cidade", "—"),
                "max_results": data.get("max_results", 0),
                "stats": data.get("stats", {}),
                "total": len(data.get("leads", [])),
                "trashed": data.get("trashed", False),
            }
            (trashed if data.get("trashed") else active).append(entry)
        except Exception:
            pass
    return {"history": active, "trashed": trashed, "count": len(active)}


@app.get("/api/history/{scan_id}")
async def get_history_scan(scan_id: str):
    """Retorna todos os dados de uma varredura específica."""
    sb = _get_sb()
    if sb:
        try:
            _id = scan_id
            res = await _sb(lambda: sb.table("scans").select("*").eq("scan_id", _id).execute())
            if res.data:
                row = res.data[0]
                return {**row, "id": row["scan_id"]}
        except Exception:
            pass
    f = HISTORY_DIR / f"{scan_id}.json"
    if not f.exists():
        raise HTTPException(status_code=404, detail="Varredura não encontrada")
    return json.loads(f.read_text(encoding="utf-8"))


@app.get("/api/history/{scan_id}/export")
async def export_history_csv(scan_id: str):
    """Exporta CSV de uma varredura do histórico."""
    from fastapi.responses import Response

    data = None
    sb = _get_sb()
    if sb:
        try:
            _id = scan_id
            res = await _sb(lambda: sb.table("scans").select("*").eq("scan_id", _id).execute())
            if res.data:
                row = res.data[0]
                data = {**row, "id": row["scan_id"]}
        except Exception:
            pass

    if data is None:
        f = HISTORY_DIR / f"{scan_id}.json"
        if not f.exists():
            raise HTTPException(status_code=404, detail="Varredura não encontrada")
        data = json.loads(f.read_text(encoding="utf-8"))

    leads = data.get("leads", [])
    cidade_slug = (data.get("cidade") or "scan").replace(" ", "-").replace("/", "-")[:30]
    date_slug = (data.get("scan_time") or "").replace("/", "").replace(":", "").replace(" ", "-").replace("às", "")[:14]
    filename = f"leads-{cidade_slug}-{date_slug}.csv".strip("-")

    fieldnames = ["name", "phone", "whatsapp_link", "address", "website", "instagram",
                  "ig_url", "ig_followers", "ig_bio", "status", "motivo", "rating"]

    def _clean(row: dict) -> dict:
        cleaned = {}
        for k, v in row.items():
            if isinstance(v, str):
                v = v.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip()
            cleaned[k] = v
        cleaned["whatsapp_link"] = _wa_link(cleaned.get("phone", ""))
        return cleaned

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(_clean(l) for l in leads)
    content = output.getvalue().encode("utf-8-sig")

    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
