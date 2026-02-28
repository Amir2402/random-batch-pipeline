from pydantic import BaseModel, EmailStr, IPvAnyAddress, HttpUrl, field_validator
from typing import List
from uuid import UUID
from datetime import date
from typing import Optional


class sales_record(BaseModel):
    id: UUID
    username: str
    password: str

    email: EmailStr
    company_email: Optional[EmailStr]

    name: str
    first_name: str
    last_name: str
    full_name: str
    prefix: Optional[str]
    suffix: Optional[str]

    phone: str
    cell: Optional[str]

    address: str
    street_address: str
    city: str
    state: str
    postal_code: str
    country: str

    latitude: float
    longitude: float
    timezone: str

    dob: date
    age: int
    gender: str

    job: str
    company: str

    ssn: str
    credit_card: str
    credit_card_provider: str
    iban: str

    ipv4: IPvAnyAddress
    ipv6: IPvAnyAddress
    mac_address: str
    user_agent: str

    url: HttpUrl
    domain: str
    picture: HttpUrl
    avatar: HttpUrl

    uuid: UUID
    md5: str
    sha1: str
    sha256: str
    locale: str

    quantity: int
    product_id: str
    product_name: str
    unit_price: float
    event_ts: str


class sales_data(BaseModel):
    sales_data: List[sales_record]

    @field_validator("sales_data")
    def validate_results(cls, v):
        if len(v) == 0:
            raise ValueError("API is returning empty list!") 
        return v 

