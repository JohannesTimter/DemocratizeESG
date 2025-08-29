from dataclasses import dataclass
from enum import Enum

class Topic(str, Enum):
    ESG = "ESG"
    FINANCIAL = "Financial"
    ANNUAL_REPORT = "Annual Report"
    UNKNOWN = "Unknown"

@dataclass
class CompanyReportFile:
    industry: str
    company_name: str
    period: int
    topic: Topic
    mimetype: str
    file_value: bytes

    def __init__(self, industry: str, company_name: str, period: int, topic: Topic, mimetype: str, file_value: bytes):
        self.industry = industry
        self.company_name = company_name
        self.period = period
        self.topic = topic
        self.mimetype = mimetype
        self.file_value = file_value