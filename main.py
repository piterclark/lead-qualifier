import os
import asyncio
import csv
import io
import json
from pathlib import Path

# Playwright browsers installed to /app/playwright-browsers during nixpacks build phase.
# That directory lives inside the WORKDIR /app, so it IS copied to the runtime container.
# Setting this unconditionally ensures playwright finds the browser regardless of CWD.
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/app/playwright-browsers"

from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from scrapers.maps_scraper import scrape_maps
from scrapers.site_checker import check_site
from scrapers.instagram_checker import check_instagram

app = FastAPI(title="Lead Qualifier — Psicólogos")

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

LEADS_FILE = DATA_DIR / "leads.json"
STATS_FILE = DATA_DIR / "stats.json"

# Estado global — persiste entre reconexões SSE
scan_state = {
    "running": False,
    "stop_requested": False,
    "leads": [],
    "stats": {"qualificados": 0, "descartados_site": 0, "descartados_ig": 0, "revisar": 0, "total": 0},
    "_task": None,           # asyncio.Task — persiste mesmo sem clientes SSE
    "_subscribers": [],      # asyncio.Queue por cliente SSE conectado
    "_log_buffer": [],       # últimos 300 logs para reconexão rápida
    "_cidade": "",
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


def _save_to_disk():
    try:
        LEADS_FILE.write_text(json.dumps(scan_state["leads"], ensure_ascii=False, indent=2), encoding="utf-8")
        STATS_FILE.write_text(json.dumps(scan_state["stats"], ensure_ascii=False, indent=2), encoding="utf-8")
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
        scan_state["running"] = True
        scan_state["stop_requested"] = False
        scan_state["leads"] = []
        scan_state["_log_buffer"] = []
        scan_state["_cidade"] = cidade
        scan_state["stats"] = {
            "qualificados": 0, "descartados_site": 0,
            "descartados_ig": 0, "revisar": 0, "total": 0
        }

        def log(msg: str):
            _broadcast({"type": "log", "msg": msg})

        def push_lead(lead: dict):
            scan_state["leads"].append(lead)
            scan_state["stats"]["total"] += 1
            _broadcast({"type": "lead", "lead": lead})
            _broadcast({"type": "stats", "stats": dict(scan_state["stats"])})
            if len(scan_state["leads"]) % 10 == 0:
                _save_to_disk()

        log(f"◈ SISTEMA INICIADO — buscando psicólogos em {cidade}")
        log("◈ Abrindo Google Maps (modo stealth)...")

        results_queue: asyncio.Queue = asyncio.Queue()

        async def maps_callback(lead: dict):
            await results_queue.put(lead)

        maps_task = asyncio.create_task(
            scrape_maps(
                search_term=f"psicóloga {cidade}",
                max_results=max_results,
                on_result=maps_callback,
            )
        )

        processed = 0
        while not maps_task.done() or not results_queue.empty():
            if scan_state["stop_requested"]:
                maps_task.cancel()
                log("◈ Varredura interrompida pelo operador.")
                break

            try:
                lead = await asyncio.wait_for(results_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            processed += 1
            log(f"  → [{processed}/{max_results}] Analisando: {lead.get('name', '?')}")

            # Etapa 2: site
            if lead.get("website"):
                has_site = await check_site(lead["website"])
                if has_site:
                    lead["status"] = "DESCARTADO"
                    lead["motivo"] = "Possui site"
                    scan_state["stats"]["descartados_site"] += 1
                    push_lead(lead)
                    continue

            # Etapa 3: Instagram
            ig_username = lead.get("instagram", "").strip().lstrip("@")
            if ig_username:
                log(f"    → @{ig_username} — verificando Instagram...")
                loop = asyncio.get_event_loop()
                ig_result = await loop.run_in_executor(None, check_instagram, ig_username)
                lead["ig_followers"] = ig_result.get("followers", 0)
                lead["ig_bio"] = ig_result.get("bio", "")[:120]
                lead["ig_url"] = f"https://instagram.com/{ig_username}"

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

            # Qualificado
            lead["status"] = "QUALIFICADO"
            lead["motivo"] = (
                "Sem site" +
                (f" — IG fraco (@{ig_username})" if ig_username else " e sem Instagram")
            )
            scan_state["stats"]["qualificados"] += 1
            push_lead(lead)

        try:
            await maps_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log(f"◈ ERRO MAPS SCRAPER: {type(e).__name__}: {e}")

        _save_to_disk()
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

@app.get("/api/health")
async def health():
    """Diagnóstico do ambiente — verifica Playwright browser e Chromium do sistema."""
    import glob as _glob, subprocess
    from scrapers.maps_scraper import _find_chromium, _SYSTEM_CHROMIUM

    browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", os.path.expanduser("~/.cache/ms-playwright"))
    pb_dir = Path(browsers_path)
    pb_exists = pb_dir.exists()
    pb_top = sorted(str(p.name) for p in pb_dir.iterdir()) if pb_exists else []
    chrome_bins = _glob.glob(f"{browsers_path}/**/chrome*", recursive=True) if pb_exists else []

    system_chromium = _SYSTEM_CHROMIUM or _find_chromium()

    usr_chromium = [p for p in ["/usr/bin/chromium", "/usr/bin/chromium-browser",
                                 "/usr/local/bin/chromium", "/usr/local/bin/chromium-browser"] if Path(p).exists()]
    try:
        which_out = subprocess.check_output(["which", "chromium"], stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        which_out = None

    app_contents = sorted(str(p.name) for p in Path("/app").iterdir()) if Path("/app").exists() else []

    ok = bool(chrome_bins) or bool(system_chromium)
    return {
        "ok": ok,
        "PLAYWRIGHT_BROWSERS_PATH": browsers_path,
        "pb_dir_exists": pb_exists,
        "pb_top_level": pb_top,
        "playwright_bins": chrome_bins[:5],
        "system_chromium": system_chromium,
        "usr_chromium": usr_chromium,
        "which_chromium": which_out,
        "PATH": os.environ.get("PATH", "")[:300],
        "app_dir": app_contents,
    }


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/status")
async def status():
    """Estado atual da varredura — para polling sem SSE."""
    return {
        "running": scan_state["running"],
        "cidade": scan_state["_cidade"],
        "stats": scan_state["stats"],
        "lead_count": len(scan_state["leads"]),
        "stop_requested": scan_state["stop_requested"],
    }


@app.get("/api/stop")
async def stop_scan():
    scan_state["stop_requested"] = True
    return {"ok": True}


@app.get("/api/leads")
async def get_leads(status_filter: str = "ALL"):
    """Retorna todos os leads em JSON (para overnight_scan.py)."""
    leads = scan_state["leads"]
    if status_filter != "ALL":
        leads = [l for l in leads if l.get("status") == status_filter]
    return {"leads": leads, "stats": scan_state["stats"]}


@app.get("/api/scan")
async def scan(
    cidade: str = Query(...),
    max_results: int = Query(default=200, le=2500),
):
    """
    SSE stream — inicia varredura de fundo ou conecta à varredura em curso.
    O scan persiste mesmo se o browser fechar.
    """
    # Iniciar task de fundo se não estiver rodando
    task_done = scan_state["_task"] is None or scan_state["_task"].done()
    if not scan_state["running"] and task_done:
        scan_state["_task"] = asyncio.create_task(_run_scan(cidade, max_results))

    # Fila exclusiva para este cliente SSE
    q: asyncio.Queue = asyncio.Queue(maxsize=2000)
    scan_state["_subscribers"].append(q)

    # Replay dos últimos logs para reconexão
    for event in scan_state["_log_buffer"][-100:]:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            break

    # Enviar leads já capturados para cliente que reconecta
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
                    # Keepalive SSE
                    yield ": keepalive\n\n"
                    if not scan_state["running"]:
                        break
        except GeneratorExit:
            # Browser fechou — scan continua em background
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
    """Recebe CSV existente e processa filtro via SSE."""

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


@app.get("/api/export")
async def export_csv(filter: str = "QUALIFICADO"):
    leads = scan_state["leads"]
    if filter != "ALL":
        leads = [l for l in leads if l.get("status") == filter]

    output = io.StringIO()
    fieldnames = ["name", "phone", "address", "website", "instagram", "ig_url",
                  "ig_followers", "ig_bio", "status", "motivo", "rating"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(leads)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{filter.lower()}.csv"},
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
