"""
DataStream AI - Privacy & PII Protection Module
Implements PII detection, redaction/masking, and data governance concepts.
*Note: This is a prototype/demo implementation for portfolio demonstration,
not a formal legal compliance certification.*
"""

import re
from typing import Dict, Any, List, Optional
import pandas as pd


def mask_email(email: Optional[str]) -> str:
    """Masks email address e.g. debasmita.roy@gmail.com -> d*********y@gmail.com"""
    if not email or not isinstance(email, str) or "@" not in email:
        return str(email) if email is not None else ""

    parts = email.split("@")
    user, domain = parts[0], parts[1]
    if len(user) <= 2:
        masked_user = user[0] + "*"
    else:
        masked_user = user[0] + ("*" * (len(user) - 2)) + user[-1]
    return f"{masked_user}@{domain}"


def mask_phone(phone: Optional[str]) -> str:
    """Masks phone numbers e.g. +91-98765-43210 -> +91-***-***-3210"""
    if not phone or not isinstance(phone, str):
        return str(phone) if phone is not None else ""

    digits = re.sub(r"[^\d]", "", phone)
    if len(digits) < 4:
        return "***"
    last_four = digits[-4:]
    return f"***-***-{last_four}"


def mask_ip(ip: Optional[str]) -> str:
    """Masks IPv4 address e.g. 192.168.1.45 -> 192.168.*.*"""
    if not ip or not isinstance(ip, str) or "." not in ip:
        return str(ip) if ip is not None else ""
    segments = ip.split(".")
    if len(segments) == 4:
        return f"{segments[0]}.{segments[1]}.*.*"
    return ip


def mask_dataframe(df: pd.DataFrame, mask_emails: bool = True, mask_phones: bool = True, mask_ips: bool = True) -> pd.DataFrame:
    """Returns a copy of the dataframe with PII attributes masked."""
    df_copy = df.copy()

    for col in df_copy.columns:
        col_lower = col.lower()
        if mask_emails and ("email" in col_lower or "mail" in col_lower):
            df_copy[col] = df_copy[col].apply(mask_email)
        elif mask_phones and ("phone" in col_lower or "mobile" in col_lower or "contact" in col_lower):
            df_copy[col] = df_copy[col].apply(mask_phone)
        elif mask_ips and ("ip" in col_lower or "ip_address" in col_lower):
            df_copy[col] = df_copy[col].apply(mask_ip)

    return df_copy


def detect_pii_columns(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Inspects a dataframe to highlight columns with potential PII."""
    detected = []
    for col in df.columns:
        col_lower = col.lower()
        if "email" in col_lower or "mail" in col_lower:
            detected.append({"column": col, "pii_type": "Email Address", "risk": "High", "action": "Masked"})
        elif "phone" in col_lower or "mobile" in col_lower:
            detected.append({"column": col, "pii_type": "Phone Number", "risk": "High", "action": "Masked"})
        elif "ip" in col_lower:
            detected.append({"column": col, "pii_type": "IP Address (Network Identifier)", "risk": "Medium", "action": "Truncated / Masked"})
        elif "name" in col_lower and col_lower != "plan_name":
            detected.append({"column": col, "pii_type": "Full Name", "risk": "Medium", "action": "Pseudonymized / Access-Controlled"})
    return detected


def get_governance_principles() -> Dict[str, Any]:
    """Returns documentation regarding data governance and DPDP principles."""
    return {
        "title": "DataStream AI Data Governance Framework",
        "disclaimer": "This is a prototype implementation demonstrating data engineering privacy patterns, not a formal legal compliance audit.",
        "principles": [
            {
                "pillar": "Purpose Limitation & Data Minimization",
                "detail": "Only essential event telemetry (timestamp, device type, watch duration) is preserved in primary operational tables. Sensitive raw identifiers are masked during ETL staging."
            },
            {
                "pillar": "Pseudonymization & Masking",
                "detail": "Email addresses and telephone digits are masked at the ingestion boundary before persisting into analytics warehouse tables or rendering on dashboards."
            },
            {
                "pillar": "Digital Personal Data Protection (DPDP) Awareness",
                "detail": "Notice, legitimate usage, consent record preservation, and right to erasure (tombstone records) simulated across user table lifecycle."
            },
            {
                "pillar": "Role-Based Access Control (RBAC)",
                "detail": "Separation of raw unstructured event storage (MongoDB) vs structured business marts with masked PII views for business analysts."
            }
        ]
    }
