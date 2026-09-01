import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from ctgov_databricks.config import API_BASE_URL, DEFAULT_PAGE_SIZE, LANDING_VOLUME, REQUEST_TIMEOUT_SECONDS
from ctgov_databricks.state import get_high_water_mark, set_high_water_mark

logger = logging.getLogger(__name__)

# Records can be updated mid-run; re-pulling the last day guards against
# missing an update that lands after this run reads it but before midnight.
OVERLAP_DAYS = 1


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _build_params(
    condition: str,
    since: str | None,
    page_size: int,
    status: str | None = None,
    phase: str | None = None,
    study_type: str | None = None,
) -> dict:
    params = {"query.cond": condition, "pageSize": page_size, "format": "json", "countTotal": "true"}
    if status:
        params["filter.overallStatus"] = status

    advanced_clauses = []
    if since:
        advanced_clauses.append(f"AREA[LastUpdatePostDate]RANGE[{since},MAX]")
    if phase:
        advanced_clauses.append(f"AREA[Phase]{phase}")
    if study_type:
        advanced_clauses.append(f"AREA[StudyType]{study_type}")
    if advanced_clauses:
        params["filter.advanced"] = " AND ".join(advanced_clauses)

    return params


def run_extract(
    spark,
    condition: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    full_refresh: bool = False,
    status: str | None = None,
    phase: str | None = None,
    study_type: str | None = None,
) -> Path:
    since = None if full_refresh else get_high_water_mark(spark, condition)
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_dir = Path(LANDING_VOLUME) / condition / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    session = _build_session()
    params = _build_params(condition, since, page_size, status=status, phase=phase, study_type=study_type)

    page = 0
    total_count = None
    while True:
        resp = session.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        total_count = total_count or data.get("totalCount")
        (out_dir / f"page_{page:04d}.json").write_text(json.dumps(data))
        logger.info("condition=%s page=%d studies=%d", condition, page, len(data.get("studies", [])))

        token = data.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
        page += 1

    manifest = {
        "condition": condition,
        "since": since,
        "full_refresh": full_refresh,
        "status": status,
        "phase": phase,
        "study_type": study_type,
        "page_count": page + 1,
        "total_count": total_count,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    new_high_water_mark = (date.today() - timedelta(days=OVERLAP_DAYS)).isoformat()
    set_high_water_mark(spark, condition, new_high_water_mark)

    return out_dir
