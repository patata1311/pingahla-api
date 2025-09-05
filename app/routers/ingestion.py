from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from io import BytesIO
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Tuple,Optional
import logging, uuid, io
from app.db import get_db
from app.models import Department, Job, Employee
from app.core.config import get_settings

router = APIRouter()
MAX_ROWS = 10000
logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

# -------- utilidades --------
READ_CSV_KW = dict(
    dtype=str,
    keep_default_na=False,
    na_values=["", " ", "NA", "NaN", "nan", "NULL", "Null", "None", "none"],
)

def _read_csv_path(filename: str, offset: int = 0, limit: int | None = None):
    settings = get_settings()
    path = (settings.data_path / filename).resolve()
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"No existe {path}")
    if limit is not None:
        df = pd.read_csv(path, skiprows=range(1, offset + 1), nrows=limit, **READ_CSV_KW)
    else:
        df = pd.read_csv(path, **READ_CSV_KW)
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        total = max(sum(1 for _ in f) - 1, 0)
    return df, total

def _read_csv_upload(file: UploadFile, offset: int = 0, limit: int | None = None):
    raw = file.file.read()
    df_full = pd.read_csv(io.BytesIO(raw), **READ_CSV_KW)
    total = len(df_full)
    df = df_full.iloc[offset: offset + limit if limit is not None else None]
    return df, total
# --- helpers de normalización/parseo ---
def _normalize_name(s: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (first_name, last_name). Si el nombre viene vacío o 'nan/null', retorna (None, None).
    """
    if s is None:
        return None, None
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None, None
    parts = s.split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]
def _to_date_safe(s: str):
    """Devuelve date o None. Nunca NaT."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # ISO: 2021-07-27T16:02:08Z, 2021-07-27 16:02:08, 2021-07-27
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass
    ts = pd.to_datetime(s, errors="coerce")  # -> Timestamp o NaT
    if pd.isna(ts):
        return None
    # si viene Timestamp/datetime:
    try:
        return ts.date() if hasattr(ts, "date") else None
    except Exception:
        return None
def _safe_int(x) -> int | None:
    """Convierte de forma segura a int; None si vacío/NaN/no convertible."""
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None
        return int(float(s)) if "." in s else int(s)
    except Exception:
        return None
def _validate_len(df: pd.DataFrame):
    if len(df) == 0:
        raise HTTPException(status_code=422, detail="El CSV no tiene filas")
    if len(df) > MAX_ROWS:
        raise HTTPException(status_code=422, detail=f"El CSV de la petición debe tener entre 1 y {MAX_ROWS} filas")


# -------- departments.csv --------
REQUIRED_DEPARTMENTS = ["id", "department"]

def _ingest_departments(df: pd.DataFrame, db: Session) -> dict:
    missing = [c for c in REQUIRED_DEPARTMENTS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Faltan columnas {missing}")
    created, updated = 0, 0
    for _, row in df.iterrows():
        dep = db.get(Department, int(row["id"]))
        if dep is None:
            dep = Department(id=int(row["id"]), name=str(row["department"]))
            db.add(dep)
            created += 1
        else:
            if dep.name != str(row["department"]):
                dep.name = str(row["department"])
                updated += 1
    db.commit()
    return {"rows": len(df), "created": created, "updated": updated}

@router.post("/ingestion/departments/csv", tags=["Ingestion"], summary="Subir departments.csv (multipart)")
def ingest_departments_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    df = _read_csv_upload(file)
    return _ingest_departments(df, db)

@router.post("/ingestion/departments/file/{filename}", tags=["Ingestion"], summary="Leer departments.csv desde DATA_DIR")
def ingest_departments_file(filename: str, db: Session = Depends(get_db)):
    df = _read_csv_path(filename)
    return _ingest_departments(df, db)

# -------- jobs.csv --------
REQUIRED_JOBS = ["id", "job"]

def _ingest_jobs(df: pd.DataFrame, db: Session) -> dict:
    missing = [c for c in REQUIRED_JOBS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Faltan columnas {missing}")
    created, updated = 0, 0
    for _, row in df.iterrows():
        job = db.get(Job, int(row["id"]))
        if job is None:
            job = Job(id=int(row["id"]), title=str(row["job"]))
            db.add(job)
            created += 1
        else:
            if job.title != str(row["job"]):
                job.title = str(row["job"])
                updated += 1
    db.commit()
    return {"rows": len(df), "created": created, "updated": updated}

@router.post("/ingestion/jobs/csv", tags=["Ingestion"], summary="Subir jobs.csv (multipart)")
def ingest_jobs_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    df = _read_csv_upload(file)
    return _ingest_jobs(df, db)

@router.post("/ingestion/jobs/file/{filename}", tags=["Ingestion"], summary="Leer jobs.csv desde DATA_DIR")
def ingest_jobs_file(filename: str, db: Session = Depends(get_db)):
    df = _read_csv_path(filename)
    return _ingest_jobs(df, db)
@router.get("/ingestion/ping", tags=["Ingestion"], summary="Ping de ingesta")
def ingestion_ping():
    return {"msg": "ingestion ok"}

# -------- hired_employees.csv --------
REQUIRED_HIRED = ["id", "name", "datetime", "department_id", "job_id"]

def _split_name(fullname: str) -> tuple[str, str]:
    parts = str(fullname).strip().split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]

def _to_date(s: str):
    s = str(s).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return pd.to_datetime(s, errors="coerce").date()
        except Exception:
            return None

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _start_batch(prefix: str) -> tuple[str, Path]:
    """Crea un batch_id y directorio de errores para esta corrida."""
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    batch_id = f"{ts}_{uuid.uuid4().hex[:8]}"
    settings = get_settings()
    err_dir = settings.data_path / "errors" / prefix
    _ensure_dir(err_dir)
    return batch_id, err_dir

def _write_rejected_csv(err_dir: Path, batch_id: str, rows: list[dict]) -> Path | None:
    if not rows:
        return None
    df_err = pd.DataFrame(rows)
    out = err_dir / f"rejected_{batch_id}.csv"
    df_err.to_csv(out, index=False, encoding="utf-8")
    return out

def _ingest_hired(df: pd.DataFrame, db: Session) -> dict:
    # columnas requeridas
    missing = [c for c in ["id","name","datetime","department_id","job_id"] if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Faltan columnas {missing}")

    batch_id, err_dir = _start_batch("hired_employees")

    created = updated = 0
    skipped_missing_fk = 0
    skipped_bad_row = 0
    skipped_dup_identity = 0

    rejected_rows: list[dict] = []   # ← aquí acumulamos los descartes

    for idx, row in df.iterrows():
        # valores ya normalizados como str por los read_csv kwargs
        emp_id = _safe_int(row.get("id"))
        dep_id = _safe_int(row.get("department_id"))
        job_id = _safe_int(row.get("job_id"))
        hire_date = _to_date_safe(row.get("datetime"))
        first, last = _normalize_name(row.get("name"))

        def reject(reason: str):
            nonlocal rejected_rows
            # guarda “payload” mínimo + motivo
            rejected_rows.append({
                "reason": reason,
                "row_index": int(idx),
                "id": row.get("id"),
                "name": row.get("name"),
                "datetime": row.get("datetime"),
                "department_id": row.get("department_id"),
                "job_id": row.get("job_id"),
            })
            logger.info("reject_row", extra={
                "table": "employees", "batch_id": batch_id, "reason": reason, "row_index": int(idx)
            })

        # reglas de validez
        if emp_id is None or hire_date is None or first is None:
            skipped_bad_row += 1
            reject("invalid_id_or_date_or_name")
            continue

        if dep_id is None or job_id is None:
            skipped_missing_fk += 1
            reject("missing_fk_values")
            continue

        if db.get(Department, dep_id) is None or db.get(Job, job_id) is None:
            skipped_missing_fk += 1
            reject("fk_not_found")
            continue

        # UPSERT idempotente
        emp = db.get(Employee, emp_id)
        if emp is None:
            emp = Employee(
                id=emp_id,
                first_name=first,
                last_name=last or "",
                hire_date=hire_date,
                salary=None,
                department_id=dep_id,
                job_id=job_id,
            )
            db.add(emp)
            try:
                db.flush()  # detecta dup de unique antes del commit
                created += 1
            except Exception:
                db.rollback()
                skipped_dup_identity += 1
                reject("duplicate_unique_identity")
                continue
        else:
            emp.first_name = first
            emp.last_name = last or ""
            emp.hire_date = hire_date
            emp.department_id = dep_id
            emp.job_id = job_id
            updated += 1

    db.commit()

    rejected_file = _write_rejected_csv(err_dir, batch_id, rejected_rows)
    return {
        "rows": len(df),
        "created": created,
        "updated": updated,
        "skipped_missing_fk": skipped_missing_fk,
        "skipped_bad_row": skipped_bad_row,
        "skipped_dup_identity": skipped_dup_identity,
        "batch_id": batch_id,
        "rejected_file": str(rejected_file) if rejected_file else None,
    }
    
@router.post("/ingestion/hired/csv", tags=["Ingestion"], summary="Subir hired_employees por lotes (multipart)")
def ingest_hired_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    offset: int = 0,
    limit: int = MAX_ROWS,
):
    if limit <= 0 or limit > MAX_ROWS:
        raise HTTPException(status_code=422, detail=f"limit debe ser 1..{MAX_ROWS}")
    df, total = _read_csv_upload(file, offset=offset, limit=limit)
    _validate_len(df)  # valida el tamaño del lote
    result = _ingest_hired(df, db)
    result.update({"offset": offset, "limit": limit, "total": total})
    return result


@router.post("/ingestion/hired/file/{filename}", tags=["Ingestion"], summary="Leer hired_employees desde DATA_DIR por lotes")
def ingest_hired_file(
    filename: str,
    db: Session = Depends(get_db),
    offset: int = 0,
    limit: int = MAX_ROWS,
):
    if limit <= 0 or limit > MAX_ROWS:
        raise HTTPException(status_code=422, detail=f"limit debe ser 1..{MAX_ROWS}")
    df, total = _read_csv_path(filename, offset=offset, limit=limit)
    _validate_len(df)
    result = _ingest_hired(df, db)
    result.update({"offset": offset, "limit": limit, "total": total})
    return result
