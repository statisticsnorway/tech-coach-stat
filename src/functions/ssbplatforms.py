import os


def is_dapla_lab() -> bool:
    region = os.getenv("DAPLA_REGION")
    return True if region and region == "DAPLA_LAB" else False


def is_old_dapla() -> bool:
    region = os.getenv("DAPLA_REGION")
    return True if region and region == "BIP" else False


def is_on_prem() -> bool:
    region = os.getenv("DAPLA_REGION")
    return True if region and region == "ON_PREM" else False


def is_dapla() -> bool:
    return is_old_dapla() or is_dapla_lab()
