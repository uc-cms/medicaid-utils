"""Fixtures that create synthetic CMS Medicaid MAX parquet data for
IP, OT, and PS preprocessing tests."""

import os

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_sorted_parquet(df: pd.DataFrame, path: str, index_col: str = "BENE_MSIS"):
    """Sort by *index_col*, write a single-file parquet under *path*/."""
    df = df.sort_values(index_col).reset_index(drop=True)
    os.makedirs(path, exist_ok=True)
    df.to_parquet(os.path.join(path, "part.0.parquet"), index=False, engine="pyarrow")


# ---------------------------------------------------------------------------
# Shared identifiers
# ---------------------------------------------------------------------------

_BENE_IDS = [f"BID{str(i).zfill(4)}" for i in range(1, 21)]
_MSIS_IDS = [f"MID{str(i).zfill(4)}" for i in range(1, 21)]
_BENE_MSIS = [f"WY-1-BID{str(i).zfill(4)}" for i in range(1, 21)]


# ---------------------------------------------------------------------------
# MAX IP fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def max_ip_data_dir(tmp_path):
    """Create a synthetic MAX IP parquet dataset and return the data_root path."""
    n = 20
    data_root = str(tmp_path / "data")
    pq_dir = os.path.join(data_root, "medicaid", "2012", "WY", "max", "ip", "parquet")

    dob = ["19800115"] * 10 + ["19900601"] * 8 + [""] + ["19700301"]
    sex = (["F"] * 8 + ["M"] * 8 + ["U"] * 2 + ["F"] + ["M"])

    admsn = [
        "20120110", "20120215", "20120310", "20120415", "20120510",
        "20120615", "20120710", "20120815", "20120910", "20121010",
        "20120110", "20120215", "20120310", "20120415", "20120510",
        "20120615", "20120710", "20120815",
        "",           # missing admission date
        "20120501",
    ]
    srvc_bgn = [d if d else "20120101" for d in admsn]
    srvc_end = [
        "20120115", "20120220", "20120315", "20120420", "20120515",
        "20120620", "20120715", "20120820", "20120915", "20121015",
        "20120115", "20120220", "20120315", "20120420", "20120515",
        "20120620", "20120715", "20120820",
        "20120110",
        "20120510",
    ]
    prcdr_dt = srvc_bgn[:]

    diag = {f"DIAG_CD_{i}": ["j189"] * n for i in range(1, 10)}
    prcdr = {f"PRCDR_CD_{i}": ["99283"] * n for i in range(1, 7)}
    prcdr_sys = {f"PRCDR_CD_SYS_{i}": ["01"] * n for i in range(1, 7)}

    # Revenue-centre columns for ED detection
    ub92 = {}
    for i in range(1, 24):
        vals = ["0000"] * n
        ub92[f"UB_92_REV_CD_GP_{i}"] = vals
    # Row 0 gets an ED revenue code in group 1
    ub92["UB_92_REV_CD_GP_1"][0] = "0450"
    # Row 1 gets 0981
    ub92["UB_92_REV_CD_GP_2"][1] = "0981"

    php_type = ["88"] * n
    type_clm = ["1"] * n
    # Row 2: encounter claim (PHP_TYPE=77)
    php_type[2] = "77"
    # Row 3: capitation claim (PHP_TYPE=88, TYPE_CLM_CD=2)
    type_clm[3] = "2"
    # Row 4: encounter via 88+3
    type_clm[4] = "3"

    rcpnt_dlvry = ["0"] * n
    rcpnt_dlvry[5] = "1"  # delivery claim

    max_tos = ["1"] * n
    max_tos[6] = "11"  # ed_tos

    mdcd_pymt = [100.0 + i * 10 for i in range(n)]
    tp_pymt = [20.0] * n

    # Create a duplicate of row 10 at row 11 (same BENE_MSIS, same dates, same everything)
    bene_msis = list(_BENE_MSIS)
    bene_ids = list(_BENE_IDS)
    msis_ids = list(_MSIS_IDS)
    bene_msis[11] = bene_msis[10]
    bene_ids[11] = bene_ids[10]
    msis_ids[11] = msis_ids[10]
    admsn[11] = admsn[10]
    srvc_bgn[11] = srvc_bgn[10]
    srvc_end[11] = srvc_end[10]
    prcdr_dt[11] = prcdr_dt[10]
    for k in diag:
        diag[k][11] = diag[k][10]
    for k in prcdr:
        prcdr[k][11] = prcdr[k][10]
    for k in prcdr_sys:
        prcdr_sys[k][11] = prcdr_sys[k][10]
    for k in ub92:
        ub92[k][11] = ub92[k][10]
    php_type[11] = php_type[10]
    type_clm[11] = type_clm[10]
    rcpnt_dlvry[11] = rcpnt_dlvry[10]
    max_tos[11] = max_tos[10]
    mdcd_pymt[11] = mdcd_pymt[10]
    tp_pymt[11] = tp_pymt[10]

    # Overlapping IP claims: rows 12 & 13 share same BENE_MSIS with overlapping dates
    bene_msis[13] = bene_msis[12]
    bene_ids[13] = bene_ids[12]
    msis_ids[13] = msis_ids[12]

    df = pd.DataFrame(
        {
            "BENE_MSIS": bene_msis,
            "BENE_ID": bene_ids,
            "MSIS_ID": msis_ids,
            "STATE_CD": ["WY"] * n,
            "YR_NUM": [2012] * n,
            "EL_DOB": dob,
            "EL_SEX_CD": sex,
            "ADMSN_DT": admsn,
            "SRVC_BGN_DT": srvc_bgn,
            "SRVC_END_DT": srvc_end,
            "PRNCPL_PRCDR_DT": prcdr_dt,
            "EL_DOD": [""] * n,
            "MDCR_DOD": [""] * n,
            **diag,
            **prcdr,
            **prcdr_sys,
            **ub92,
            "PHP_TYPE": php_type,
            "TYPE_CLM_CD": type_clm,
            "RCPNT_DLVRY_CD": rcpnt_dlvry,
            "MAX_TOS": max_tos,
            "MDCD_PYMT_AMT": mdcd_pymt,
            "TP_PYMT_AMT": tp_pymt,
        }
    )

    _write_sorted_parquet(df, pq_dir)
    return data_root


# ---------------------------------------------------------------------------
# MAX OT fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def max_ot_data_dir(tmp_path):
    """Create a synthetic MAX OT parquet dataset and return the data_root path."""
    n = 20
    data_root = str(tmp_path / "data")
    pq_dir = os.path.join(data_root, "medicaid", "2012", "WY", "max", "ot", "parquet")

    dob = ["19800115"] * 10 + ["19900601"] * 8 + [""] + ["19700301"]
    sex = ["F"] * 8 + ["M"] * 8 + ["U"] * 2 + ["F", "M"]

    srvc_bgn = [
        "20120110", "20120215", "20120310", "20120415", "20120510",
        "20120615", "20120710", "20120815", "20120910", "20121010",
        "20120110", "20120215", "20120310", "20120415", "20120510",
        "20120615", "20120710", "20120815",
        "",           # missing srvc_bgn_date
        "20120501",
    ]
    srvc_end = [
        "20120112", "20120217", "20120312", "20120417", "20120512",
        "20120617", "20120712", "20120817", "20120912", "20121012",
        "20120112", "20120217", "20120312", "20120417", "20120512",
        "20120617", "20120712", "20120817",
        "20120110",
        "20120503",
    ]

    diag1 = ["j189"] * n
    diag2 = ["e119"] * n

    prcdr_cd = ["99213"] * n
    prcdr_cd[7] = "D0120"   # dental procedure code
    prcdr_cd_sys = ["1"] * n

    php_type = ["88"] * n
    type_clm = ["1"] * n
    php_type[2] = "77"   # encounter
    type_clm[3] = "2"    # capitation
    type_clm[4] = "3"    # encounter via 88+3

    max_tos = ["1"] * n
    max_tos[5] = "9"     # dental TOS
    max_tos[6] = "26"    # transport TOS

    plc_of_srvc = ["11"] * n
    plc_of_srvc[8] = "23"   # ED POS
    plc_of_srvc[9] = "41"   # ambulance land
    plc_of_srvc[14] = "42"  # ambulance air

    mdcd_pymt = [50.0 + i * 5 for i in range(n)]
    tp_pymt = [10.0] * n

    # Duplicate row: row 11 = copy of row 10
    bene_msis = list(_BENE_MSIS)
    bene_ids = list(_BENE_IDS)
    msis_ids = list(_MSIS_IDS)
    bene_msis[11] = bene_msis[10]
    bene_ids[11] = bene_ids[10]
    msis_ids[11] = msis_ids[10]
    srvc_bgn[11] = srvc_bgn[10]
    srvc_end[11] = srvc_end[10]
    diag1[11] = diag1[10]
    diag2[11] = diag2[10]
    prcdr_cd[11] = prcdr_cd[10]
    prcdr_cd_sys[11] = prcdr_cd_sys[10]
    php_type[11] = php_type[10]
    type_clm[11] = type_clm[10]
    max_tos[11] = max_tos[10]
    plc_of_srvc[11] = plc_of_srvc[10]
    mdcd_pymt[11] = mdcd_pymt[10]
    tp_pymt[11] = tp_pymt[10]

    # UB_92_REV_CD for OT - single column, not grouped
    ub92_rev_cd = ["0000"] * n
    ub92_rev_cd[0] = "0450"   # ED rev code
    ub92_rev_cd[1] = "0981"   # ED rev code

    df = pd.DataFrame(
        {
            "BENE_MSIS": bene_msis,
            "BENE_ID": bene_ids,
            "MSIS_ID": msis_ids,
            "STATE_CD": ["WY"] * n,
            "YR_NUM": [2012] * n,
            "EL_DOB": dob,
            "EL_SEX_CD": sex,
            "SRVC_BGN_DT": srvc_bgn,
            "SRVC_END_DT": srvc_end,
            "DIAG_CD_1": diag1,
            "DIAG_CD_2": diag2,
            "PRCDR_CD": prcdr_cd,
            "PRCDR_CD_SYS": prcdr_cd_sys,
            "PHP_TYPE": php_type,
            "TYPE_CLM_CD": type_clm,
            "MAX_TOS": max_tos,
            "PLC_OF_SRVC_CD": plc_of_srvc,
            "UB_92_REV_CD": ub92_rev_cd,
            "MDCD_PYMT_AMT": mdcd_pymt,
            "TP_PYMT_AMT": tp_pymt,
        }
    )

    _write_sorted_parquet(df, pq_dir)
    return data_root


# ---------------------------------------------------------------------------
# MAX PS fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def max_ps_data_dir(tmp_path):
    """Create a synthetic MAX PS parquet dataset and return the data_root path."""
    n = 20
    data_root = str(tmp_path / "data")
    pq_dir = os.path.join(data_root, "medicaid", "2012", "WY", "max", "ps", "parquet")

    dob = ["19800115"] * 10 + ["19900601"] * 8 + [""] + ["19700301"]
    sex = ["F"] * 8 + ["M"] * 8 + ["U"] * 2 + ["F", "M"]

    # Eligibility codes: mix of categories
    # 11-15=aged, 21-25=blind/disabled, 31-35=child, 41-45=adult, 51-55=other
    elg_base = ["31"] * 10 + ["11"] * 5 + ["21"] * 3 + ["00"] + ["41"]
    elg_cols = {}
    for mon in range(1, 13):
        vals = list(elg_base)
        # Row 0: eligibility changes mid-year (months 7-12 switch to adult)
        if mon >= 7:
            vals[0] = "41"
        # Row 18: ineligible all year
        vals[18] = "00"
        elg_cols[f"MAX_ELG_CD_MO_{mon}"] = vals

    # Restricted benefits flags
    rstrct_cols = {}
    for mon in range(1, 13):
        vals = ["1"] * n  # full scope
        # Row 1: restricted (alien) some months
        if mon <= 6:
            vals[1] = "2"
        # Row 2: restricted (dual) all year
        vals[2] = "3"
        rstrct_cols[f"EL_RSTRCT_BNFT_FLG_{mon}"] = vals

    # TANF flags
    tanf_cols = {}
    for mon in range(1, 13):
        vals = ["1"] * n  # 1 = did NOT receive TANF
        # Row 3: received TANF in months 1-6
        if mon <= 6:
            vals[3] = "2"
        tanf_cols[f"EL_TANF_CASH_FLG_{mon}"] = vals

    # Dual eligibility
    el_mdcr_ann_xovr = ["00"] * n
    el_mdcr_ann_xovr[4] = "10"  # dual
    el_mdcr_ann_xovr[5] = "99"  # should be flagged as non-dual (outside 0-9)

    # Duplicate BENE_MSIS for testing excl_duplicated_bene_id
    bene_msis = list(_BENE_MSIS)
    bene_ids = list(_BENE_IDS)
    msis_ids = list(_MSIS_IDS)
    bene_msis[17] = bene_msis[16]
    bene_ids[17] = bene_ids[16]
    msis_ids[17] = msis_ids[16]

    df = pd.DataFrame(
        {
            "BENE_MSIS": bene_msis,
            "BENE_ID": bene_ids,
            "MSIS_ID": msis_ids,
            "STATE_CD": ["WY"] * n,
            "YR_NUM": [2012] * n,
            "EL_DOB": dob,
            "EL_SEX_CD": sex,
            "EL_DOD": [""] * n,
            "MDCR_DOD": [""] * n,
            "EL_RSDNC_ZIP_CD_LTST": ["820010000"] * n,
            "EL_RSDNC_CNTY_CD_LTST": ["001"] * n,
            **elg_cols,
            **rstrct_cols,
            **tanf_cols,
            "EL_MDCR_ANN_XOVR_99": el_mdcr_ann_xovr,
            "EL_MDCR_DUAL_ANN": ["00"] * n,
        }
    )

    _write_sorted_parquet(df, pq_dir)
    return data_root


# ===========================================================================
# TAF fixtures
# ===========================================================================

def _taf_bene_msis(n: int = 15) -> list:
    """Return a sorted list of BENE_MSIS identifiers for TAF tests."""
    return sorted([f"AL-1-BENE{str(i).zfill(4)}" for i in range(1, n + 1)])


def _taf_write_parquet(df: pd.DataFrame, dest_dir: str) -> None:
    """Sort by BENE_MSIS then write a single-file parquet dataset."""
    if "BENE_MSIS" in df.columns:
        df = df.sort_values("BENE_MSIS").reset_index(drop=True)
    os.makedirs(dest_dir, exist_ok=True)
    df.to_parquet(
        os.path.join(dest_dir, "part.0.parquet"), index=False, engine="pyarrow"
    )


# ---------------------------------------------------------------------------
# TAF IP fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def taf_ip_data(tmp_path):
    """Create synthetic TAF IP parquet data.

    Returns (data_root, year, state, tmp_folder).
    """
    year, state = 2019, "AL"
    data_root = str(tmp_path / "cms_data")
    tmp_folder = str(tmp_path / "tmp_cache")
    os.makedirs(tmp_folder, exist_ok=True)

    base_dir = os.path.join(
        data_root, "medicaid", str(year), state, "taf", "ip"
    )
    bene_msis = _taf_bene_msis(15)

    # -- base (iph) ---------------------------------------------------------
    base_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1960 + (i % 40)
        mon = (i % 12) + 1
        srvc_bgn = f"{year}{str(mon).zfill(2)}01"
        srvc_end = f"{year}{str(mon).zfill(2)}15"
        rec = {
            "BENE_ID": f"BENE{str(i+1).zfill(4)}",
            "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
            "STATE_CD": state,
            "SUBMTG_STATE_CD": "01",
            "CLM_ID": f"CLM{str(i+1).zfill(6)}",
            "BIRTH_DT": f"{birth_yr}0115",
            "SRVC_BGN_DT": srvc_bgn,
            "SRVC_END_DT": srvc_end,
            "ADMSM_DT": srvc_bgn,
            "PRCDR_CD_DT_1": srvc_bgn,
            "ADMTG_DGNS_CD": f"Z{str(38 + i).zfill(3)}",
            "CLM_TYPE_CD": "1" if i % 3 != 2 else "3",
            "MDCD_PD_AMT": str(round(100 + i * 50.5, 2)),
            "TP_PD_AMT": str(round(10 + i * 5.0, 2)),
            "BENE_MSIS": bm,
            "DA_RUN_ID": str(1000 + i),
            "IP_VRSN": str(1),
            "IP_FIL_DT": f"T{year}{str(mon).zfill(2)}",
        }
        for d in range(1, 13):
            rec[f"DGNS_CD_{d}"] = (
                f"A{str(10 + d).zfill(2)}.{i}" if d <= 3 else ""
            )
        rec["DGNS_VRSN_CD_1"] = "0"
        for p in range(1, 7):
            rec[f"PRCDR_CD_{p}"] = f"B{str(20 + p)}" if p <= 2 else ""
            rec[f"PRCDR_CD_SYS_{p}"] = "07" if p <= 2 else ""
        base_records.append(rec)

    # edge: missing DOB
    base_records[0]["BIRTH_DT"] = ""
    # edge: missing admission date -> should be imputed from srvc_bgn
    base_records[1]["ADMSM_DT"] = ""
    # edge: missing principal procedure date
    base_records[2]["PRCDR_CD_DT_1"] = ""
    # edge: duplicate CLM_ID with lower DA_RUN_ID (should be dropped)
    dup_rec = base_records[3].copy()
    dup_rec["DA_RUN_ID"] = str(500)
    base_records.append(dup_rec)
    # edge: non-FFS/encounter CLM_TYPE_CD
    base_records[4]["CLM_TYPE_CD"] = "5"
    # edge: diagnosis codes with special chars
    base_records[5]["DGNS_CD_1"] = "a12.3"
    base_records[5]["ADMTG_DGNS_CD"] = "b45-6"
    # edge: srvc_end before admsn (invalid LOS)
    base_records[6]["SRVC_END_DT"] = f"{year}0101"
    base_records[6]["SRVC_BGN_DT"] = f"{year}0315"
    base_records[6]["ADMSM_DT"] = f"{year}0315"

    _taf_write_parquet(
        pd.DataFrame(base_records),
        os.path.join(base_dir, "iph", "parquet"),
    )

    # -- line (ipl) ----------------------------------------------------------
    line_records = []
    for i, bm in enumerate(bene_msis):
        for ln in range(1, 3):
            line_records.append(
                {
                    "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                    "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                    "STATE_CD": state,
                    "CLM_ID": f"CLM{str(i+1).zfill(6)}",
                    "LINE_NUM": str(ln),
                    "REV_CNTR_CD": f"0{str(100 + ln)}",
                    "NDC": (
                        f"1234567{str(i).zfill(4)}" if ln == 1 else ""
                    ),
                    "BENE_MSIS": bm,
                    "DA_RUN_ID": str(1000 + i),
                    "IP_VRSN": str(1),
                }
            )
    # edge: short NDC
    line_records[0]["NDC"] = "1234"
    # edge: NDC with spaces
    line_records[2]["NDC"] = " 5678 "

    _taf_write_parquet(
        pd.DataFrame(line_records),
        os.path.join(base_dir, "ipl", "parquet"),
    )

    # -- occurrence (ipoccr) -------------------------------------------------
    occr_records = []
    for i, bm in enumerate(bene_msis[:5]):
        occr_records.append(
            {
                "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                "STATE_CD": state,
                "CLM_ID": f"CLM{str(i+1).zfill(6)}",
                "BENE_MSIS": bm,
                "OCRNC_CD_1": "01",
                "OCRNC_DT_1": f"{year}0101",
            }
        )
    _taf_write_parquet(
        pd.DataFrame(occr_records),
        os.path.join(base_dir, "ipoccr", "parquet"),
    )

    return data_root, year, state, tmp_folder


# ---------------------------------------------------------------------------
# TAF OT fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def taf_ot_data(tmp_path):
    """Create synthetic TAF OT parquet data.

    Returns (data_root, year, state, tmp_folder).
    """
    year, state = 2019, "AL"
    data_root = str(tmp_path / "cms_data")
    tmp_folder = str(tmp_path / "tmp_cache")
    os.makedirs(tmp_folder, exist_ok=True)

    base_dir = os.path.join(
        data_root, "medicaid", str(year), state, "taf", "ot"
    )
    bene_msis = _taf_bene_msis(15)

    # -- base (oth) ----------------------------------------------------------
    base_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1955 + (i % 50)
        mon = (i % 12) + 1
        srvc_bgn = f"{year}{str(mon).zfill(2)}05"
        srvc_end = f"{year}{str(mon).zfill(2)}05"
        rec = {
            "BENE_ID": f"BENE{str(i+1).zfill(4)}",
            "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
            "STATE_CD": state,
            "CLM_ID": f"OCLM{str(i+1).zfill(6)}",
            "BIRTH_DT": f"{birth_yr}0610",
            "SRVC_BGN_DT": srvc_bgn,
            "SRVC_END_DT": srvc_end,
            "DGNS_CD_1": f"E{str(11 + i).zfill(2)}1",
            "DGNS_CD_2": (
                f"F{str(20 + i).zfill(2)}0" if i % 2 == 0 else ""
            ),
            "CLM_TYPE_CD": "1" if i % 4 != 3 else "3",
            "MDCD_PD_AMT": str(round(50 + i * 25.0, 2)),
            "TP_PD_AMT": str(round(5 + i * 2.5, 2)),
            "POS_CD": str(11 + (i % 10)),
            "BENE_MSIS": bm,
            "DA_RUN_ID": str(2000 + i),
            "OT_VRSN": str(1),
            "OT_FIL_DT": f"{year}{str(mon).zfill(2)}",
        }
        base_records.append(rec)

    # edge cases
    base_records[0]["BIRTH_DT"] = ""
    base_records[1]["SRVC_BGN_DT"] = ""
    base_records[2]["CLM_TYPE_CD"] = "5"
    base_records[3]["DGNS_CD_1"] = "e11.9"
    base_records[4]["SRVC_BGN_DT"] = f"{year}0620"
    base_records[4]["SRVC_END_DT"] = f"{year}0610"
    dup_rec = base_records[5].copy()
    dup_rec["DA_RUN_ID"] = str(100)
    base_records.append(dup_rec)

    _taf_write_parquet(
        pd.DataFrame(base_records),
        os.path.join(base_dir, "oth", "parquet"),
    )

    # -- line (otl) ----------------------------------------------------------
    line_records = []
    for i, bm in enumerate(bene_msis):
        for ln in range(1, 3):
            line_records.append(
                {
                    "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                    "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                    "STATE_CD": state,
                    "CLM_ID": f"OCLM{str(i+1).zfill(6)}",
                    "LINE_NUM": str(ln),
                    "LINE_PRCDR_CD": f"9921{ln}" if ln == 1 else "",
                    "LINE_PRCDR_CD_SYS": "01" if ln == 1 else "",
                    "NDC": (
                        f"9876543{str(i).zfill(4)}" if ln == 1 else ""
                    ),
                    "BENE_MSIS": bm,
                    "DA_RUN_ID": str(2000 + i),
                    "OT_VRSN": str(1),
                }
            )
    line_records[0]["LINE_PRCDR_CD"] = "99.21"

    _taf_write_parquet(
        pd.DataFrame(line_records),
        os.path.join(base_dir, "otl", "parquet"),
    )

    # -- occurrence (otoccr) -------------------------------------------------
    occr_records = []
    for i, bm in enumerate(bene_msis[:5]):
        occr_records.append(
            {
                "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                "STATE_CD": state,
                "CLM_ID": f"OCLM{str(i+1).zfill(6)}",
                "BENE_MSIS": bm,
                "OCRNC_CD_1": "01",
                "OCRNC_DT_1": f"{year}0101",
            }
        )
    _taf_write_parquet(
        pd.DataFrame(occr_records),
        os.path.join(base_dir, "otoccr", "parquet"),
    )

    return data_root, year, state, tmp_folder


# ---------------------------------------------------------------------------
# TAF PS fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def taf_ps_data(tmp_path):
    """Create synthetic TAF PS parquet data.

    Returns (data_root, year, state, tmp_folder).
    """
    year, state = 2019, "AL"
    data_root = str(tmp_path / "cms_data")
    tmp_folder = str(tmp_path / "tmp_cache")
    os.makedirs(tmp_folder, exist_ok=True)

    de_dir = os.path.join(
        data_root, "medicaid", str(year), state, "taf", "de"
    )
    bene_msis = _taf_bene_msis(15)

    # -- dates (dedts) -------------------------------------------------------
    dates_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1960 + (i % 40)
        rec = {
            "BENE_ID": f"BENE{str(i+1).zfill(4)}",
            "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
            "STATE_CD": state,
            "BIRTH_DT": f"{birth_yr}0301",
            "DEATH_DT": "",
            "BENE_MSIS": bm,
            "RFRNC_YR": str(year),
            "ENRLMT_START_DT": f"{year}0101",
            "ENRLMT_END_DT": f"{year}1231",
        }
        dates_records.append(rec)
    # death in claim year
    dates_records[0]["DEATH_DT"] = f"{year}0915"
    # missing DOB
    dates_records[1]["BIRTH_DT"] = ""
    # death after claim year
    dates_records[2]["DEATH_DT"] = f"{year + 2}0101"
    # enrollment gap
    dates_records[3]["ENRLMT_START_DT"] = f"{year}0401"
    dates_records[3]["ENRLMT_END_DT"] = f"{year}0930"
    # duplicated bene_msis
    dup_rec = dates_records[4].copy()
    dates_records.append(dup_rec)

    _taf_write_parquet(
        pd.DataFrame(dates_records),
        os.path.join(de_dir, "dedts", "parquet"),
    )

    # -- base (debse) --------------------------------------------------------
    base_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1960 + (i % 40)
        rec = {
            "BENE_ID": f"BENE{str(i+1).zfill(4)}",
            "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
            "STATE_CD": state,
            "BIRTH_DT": f"{birth_yr}0301",
            "SEX_CD": "F" if i % 3 == 0 else ("M" if i % 3 == 1 else "U"),
            "RACE_ETHNCTY_CD": str((i % 5) + 1),
            "BENE_MSIS": bm,
            "RFRNC_YR": str(year),
            "BENE_ZIP_CD": f"3620{str(i).zfill(5)}",
            "BENE_CNTY_CD": f"00{str(i % 10 + 1)}",
            "TANF_CASH_CD": "2" if i % 4 == 0 else "1",
        }
        for mon in range(1, 13):
            mm = str(mon).zfill(2)
            rec[f"ELGBLTY_GRP_CD_{mm}"] = str((i + mon) % 75).zfill(2)
            rec[f"DUAL_ELGBL_CD_{mm}"] = str(
                1 if (i % 5 == 0 and mon <= 6) else 0
            )
            rec[f"RSTRCTD_BNFTS_CD_{mm}"] = str(
                1 if i % 6 != 0 else 3
            )
            mas = (i + mon) % 6
            boe = (i + mon) % 10
            rec[f"MASBOE_CD_{mm}"] = str(mas * 100 + boe)
            rec[f"MDCD_ENRLMT_DAYS_{mm}"] = str(
                31 if mon in [1, 3, 5, 7, 8, 10, 12] else 30
            )
            rec[f"CHIP_ENRLMT_DAYS_{mm}"] = "0"
            rec[f"MISG_ENRLMT_TYPE_IND_{mm}"] = "0"
        base_records.append(rec)

    # missing DOB (matching dates fixture row 1)
    base_records[1]["BIRTH_DT"] = ""
    # duplicated bene_msis (matching dates dup) — must differ in at least
    # one column so flag_duplicates (exact-row dedup) keeps both rows
    dup_base = base_records[4].copy()
    dup_base["RACE_ETHNCTY_CD"] = "9"
    base_records.append(dup_base)
    # all restricted benefits
    for mon in range(1, 13):
        mm = str(mon).zfill(2)
        base_records[5][f"RSTRCTD_BNFTS_CD_{mm}"] = "3"
    # no enrollment
    for mon in range(1, 13):
        mm = str(mon).zfill(2)
        base_records[6][f"MDCD_ENRLMT_DAYS_{mm}"] = "0"
        base_records[6][f"MISG_ENRLMT_TYPE_IND_{mm}"] = "1"

    _taf_write_parquet(
        pd.DataFrame(base_records),
        os.path.join(de_dir, "debse", "parquet"),
    )

    return data_root, year, state, tmp_folder


# ---------------------------------------------------------------------------
# TAF RX fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def taf_rx_data(tmp_path):
    """Create synthetic TAF RX parquet data.

    Returns (data_root, year, state, tmp_folder).
    """
    year, state = 2019, "AL"
    data_root = str(tmp_path / "cms_data")
    tmp_folder = str(tmp_path / "tmp_cache")
    os.makedirs(tmp_folder, exist_ok=True)

    rx_dir = os.path.join(
        data_root, "medicaid", str(year), state, "taf", "rx"
    )
    bene_msis = _taf_bene_msis(15)

    # -- base (rxh) ----------------------------------------------------------
    base_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1965 + (i % 35)
        base_records.append(
            {
                "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                "STATE_CD": state,
                "CLM_ID": f"RCLM{str(i+1).zfill(6)}",
                "BIRTH_DT": f"{birth_yr}0720",
                "CLM_TYPE_CD": "1" if i % 3 != 2 else "3",
                "MDCD_PD_AMT": str(round(20 + i * 10.0, 2)),
                "TP_PD_AMT": str(round(2 + i * 1.0, 2)),
                "BENE_MSIS": bm,
                "DA_RUN_ID": str(3000 + i),
                "RX_VRSN": str(1),
                "RX_FIL_DT": f"{year}{str((i % 12) + 1).zfill(2)}",
            }
        )
    base_records[0]["BIRTH_DT"] = ""
    dup_rec = base_records[3].copy()
    dup_rec["DA_RUN_ID"] = str(100)
    base_records.append(dup_rec)

    _taf_write_parquet(
        pd.DataFrame(base_records),
        os.path.join(rx_dir, "rxh", "parquet"),
    )

    # -- line (rxl) ----------------------------------------------------------
    line_records = []
    for i, bm in enumerate(bene_msis):
        for ln in range(1, 3):
            line_records.append(
                {
                    "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                    "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                    "STATE_CD": state,
                    "CLM_ID": f"RCLM{str(i+1).zfill(6)}",
                    "LINE_NUM": str(ln),
                    "NDC": (
                        f"5432109{str(i).zfill(4)}" if ln == 1 else ""
                    ),
                    "NDC_QTY": str(30 + i) if ln == 1 else "",
                    "DAYS_SUPPLY": str(30) if ln == 1 else "",
                    "BENE_MSIS": bm,
                    "DA_RUN_ID": str(3000 + i),
                    "RX_VRSN": str(1),
                }
            )
    line_records[0]["NDC"] = "999"
    line_records[2]["NDC"] = " 5678 "

    _taf_write_parquet(
        pd.DataFrame(line_records),
        os.path.join(rx_dir, "rxl", "parquet"),
    )

    return data_root, year, state, tmp_folder


# ---------------------------------------------------------------------------
# TAF LT fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def taf_lt_data(tmp_path):
    """Create synthetic TAF LT parquet data.

    Returns (data_root, year, state, tmp_folder).
    """
    year, state = 2019, "AL"
    data_root = str(tmp_path / "cms_data")
    tmp_folder = str(tmp_path / "tmp_cache")
    os.makedirs(tmp_folder, exist_ok=True)

    lt_dir = os.path.join(
        data_root, "medicaid", str(year), state, "taf", "lt"
    )
    bene_msis = _taf_bene_msis(15)

    # -- base (lth) ----------------------------------------------------------
    base_records = []
    for i, bm in enumerate(bene_msis):
        birth_yr = 1940 + (i % 30)
        mon = (i % 12) + 1
        srvc_bgn = f"{year}{str(mon).zfill(2)}01"
        srvc_end = f"{year}{str(mon).zfill(2)}28"
        base_records.append(
            {
                "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                "STATE_CD": state,
                "CLM_ID": f"LCLM{str(i+1).zfill(6)}",
                "BIRTH_DT": f"{birth_yr}1125",
                "SRVC_BGN_DT": srvc_bgn,
                "SRVC_END_DT": srvc_end,
                "CLM_TYPE_CD": "1" if i % 3 != 2 else "3",
                "MDCD_PD_AMT": str(round(200 + i * 100.0, 2)),
                "TP_PD_AMT": str(round(20 + i * 10.0, 2)),
                "BENE_MSIS": bm,
                "DA_RUN_ID": str(4000 + i),
                "LT_VRSN": str(1),
                "RFRNC_YR": str(year),
            }
        )
    base_records[0]["BIRTH_DT"] = ""
    dup_rec = base_records[3].copy()
    dup_rec["DA_RUN_ID"] = str(100)
    base_records.append(dup_rec)

    _taf_write_parquet(
        pd.DataFrame(base_records),
        os.path.join(lt_dir, "lth", "parquet"),
    )

    # -- line (ltl) ----------------------------------------------------------
    line_records = []
    for i, bm in enumerate(bene_msis):
        for ln in range(1, 3):
            line_records.append(
                {
                    "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                    "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                    "STATE_CD": state,
                    "CLM_ID": f"LCLM{str(i+1).zfill(6)}",
                    "LINE_NUM": str(ln),
                    "NDC": (
                        f"1111111{str(i).zfill(4)}" if ln == 1 else ""
                    ),
                    "BENE_MSIS": bm,
                    "DA_RUN_ID": str(4000 + i),
                    "LT_VRSN": str(1),
                }
            )

    _taf_write_parquet(
        pd.DataFrame(line_records),
        os.path.join(lt_dir, "ltl", "parquet"),
    )

    # -- occurrence (ltoccr) -------------------------------------------------
    occr_records = []
    for i, bm in enumerate(bene_msis[:5]):
        occr_records.append(
            {
                "BENE_ID": f"BENE{str(i+1).zfill(4)}",
                "MSIS_ID": f"MSIS{str(i+1).zfill(4)}",
                "STATE_CD": state,
                "CLM_ID": f"LCLM{str(i+1).zfill(6)}",
                "BENE_MSIS": bm,
                "OCRNC_CD_1": "01",
                "OCRNC_DT_1": f"{year}0101",
            }
        )
    _taf_write_parquet(
        pd.DataFrame(occr_records),
        os.path.join(lt_dir, "ltoccr", "parquet"),
    )

    return data_root, year, state, tmp_folder
