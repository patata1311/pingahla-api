# app/routers/metrics.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db import get_db

router = APIRouter()

@router.get("/metrics/hired-per-quarter")
def hired_per_quarter(year: int = Query(2021, ge=1900, le=2100), db: Session = Depends(get_db)):
    sql = text("""
        SELECT d.name AS department, j.title AS job,
                SUM(CASE WHEN QUARTER(e.hire_date)=1 THEN 1 ELSE 0 END) AS Q1,
                SUM(CASE WHEN QUARTER(e.hire_date)=2 THEN 1 ELSE 0 END) AS Q2,
                SUM(CASE WHEN QUARTER(e.hire_date)=3 THEN 1 ELSE 0 END) AS Q3,
                SUM(CASE WHEN QUARTER(e.hire_date)=4 THEN 1 ELSE 0 END) AS Q4
        FROM employees e
        JOIN departments d ON d.id = e.department_id
        JOIN jobs        j ON j.id = e.job_id
        WHERE YEAR(e.hire_date) = :year
        GROUP BY d.name, j.title
        ORDER BY d.name ASC, j.title ASC
    """)
    rows = db.execute(sql, {"year": year}).mappings().all()
    return rows

@router.get("/metrics/departments-above-mean")
def departments_above_mean(year: int = Query(2021, ge=1900, le=2100), db: Session = Depends(get_db)):
    sql = text("""
        WITH hires AS (
            SELECT department_id, COUNT(*) AS hired
            FROM employees
            WHERE YEAR(hire_date) = :year
            GROUP BY department_id
        ),
        meanval AS (
            SELECT AVG(hired) AS avg_hired FROM hires
        )
        SELECT d.id, d.name AS department, h.hired
        FROM hires h
        JOIN departments d ON d.id = h.department_id
        JOIN meanval m
        WHERE h.hired > m.avg_hired
        ORDER BY h.hired DESC
    """)
    rows = db.execute(sql, {"year": year}).mappings().all()
    return rows
