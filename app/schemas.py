from datetime import date
from pydantic import BaseModel, Field, field_validator

class EmployeeIn(BaseModel):
    first_name: str = Field(min_length=1, max_length=80)
    last_name:  str = Field(min_length=1, max_length=80)
    hire_date:  date
    department: str = Field(min_length=1, max_length=120)
    job:        str = Field(min_length=1, max_length=120)
    salary:     float | None = None

    @field_validator("salary")
    @classmethod
    def non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("salary must be >= 0")
        return v
