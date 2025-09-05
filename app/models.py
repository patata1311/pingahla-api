from datetime import date
from sqlalchemy import String, Integer, Date, DECIMAL, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db import Base

class Department(Base):
    __tablename__ = "departments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    employees: Mapped[list["Employee"]] = relationship(back_populates="department")

class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    min_salary: Mapped[float] = mapped_column(DECIMAL(18, 2), nullable=True)
    max_salary: Mapped[float] = mapped_column(DECIMAL(18, 2), nullable=True)
    employees: Mapped[list["Employee"]] = relationship(back_populates="job")

class Employee(Base):
    __tablename__ = "employees"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(80), nullable=False)
    last_name: Mapped[str] = mapped_column(String(80), nullable=False)
    hire_date: Mapped[date] = mapped_column(Date, nullable=False)
    salary: Mapped[float | None] = mapped_column(DECIMAL(18, 2), nullable=True)

    department_id: Mapped[int | None] = mapped_column(ForeignKey("departments.id"), index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), index=True)

    department: Mapped["Department"] = relationship(back_populates="employees")
    job: Mapped["Job"] = relationship(back_populates="employees")

    __table_args__ = (
        UniqueConstraint("first_name", "last_name", "hire_date", name="uq_employee_identity"),
        Index("ix_employees_hire_date", "hire_date"),
    )
