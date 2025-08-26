# ftpcoverages.py
# Full Python conversion of Ftpcoverages_mdl.php (CodeIgniter model)
# Uses SQLModel/SQLAlchemy sessions + parameterized raw SQL
# by: you 🫶

from typing import Any, Dict, List, Optional, Union
from sqlmodel import create_engine, Session, text
from config import DATABASE_URL
from utils.helpers import Print

# --- Engine ---
engine = create_engine(DATABASE_URL, echo=True)


# --- Helpers -----------------------------------------------------------------


def _fetch_all_dicts(
    session: Session, sql: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = session.execute(text(sql), params).mappings().all()
    return [dict(r) for r in result]


def _fetch_scalar(session: Session, sql: str, params: Dict[str, Any]) -> int:
    row = session.exec(text(sql), params).first()
    if row is None:
        return 0
    # row may be a scalar or Mapping
    try:
        # If row is a Mapping with alias 'cnt'
        if hasattr(row, "_mapping"):
            m = row._mapping
            return int(m.get("cnt") or list(m.values())[0])
        return int(row)
    except Exception:
        return int(row[0]) if isinstance(row, (list, tuple)) else 0


def _maybe_limit_offset(limit: Union[int, str], offset: int) -> str:
    if isinstance(limit, str) and limit.lower() == "all":
        return ""
    if limit is None:
        return ""
    try:
        lim = int(limit)
    except Exception:
        return ""
    off = int(offset or 0)
    return f" LIMIT {lim} OFFSET {off} "


# --- NationGuard --------------------------------------------------------------


def nationguard_coverage_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
            r.ContractStatus AS Status,
            DATE_FORMAT((r.SaleDate),'%m/%d/%Y') AS SaleDate,
            r.ContractNumber AS ContractNo,
            r.LastName AS CustomerLName,
            r.FirstName AS CUST_FIRST_NAME,
            r.LastName AS CUST_LAST_NAME,
            r.VIN,
            r.ContractNumber AS PolicyNumber,
            r.CoverageName AS COVERAGE_NAME,
            r.ProductType AS COV_TYPE,
            r.ContractStatus AS COV_STATUS,
            r.SaleDate AS PurchaseDate,
            r.SaleDate AS EFF_DATE,
            r.TermMileage AS EFF_MILEAGE,
            'N/A' AS EXP_DATE,
            'N/A' AS EXP_MILEAGE,
            r.DeductibleAmount AS DEDUCTIBLE
        FROM mypcp_roadvant.tbl_nationgard AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.FirstName AS CUST_FIRST_NAME,
      r.LastName  AS CUST_LAST_NAME,
      r.VIN,
      r.ContractNumber AS PolicyNumber,
      r.CoverageName AS COVERAGE_NAME,
      r.ProductType AS COV_TYPE,
      r.ContractStatus AS COV_STATUS,
      r.SaleDate AS PurchaseDate,
      r.SaleDate AS EFF_DATE,
      r.TermMileage AS EFF_MILEAGE,
      'N/A' AS EXP_DATE,
      'N/A' AS EXP_MILEAGE,
      r.DeductibleAmount AS DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_nationgard AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def nationguard_coverage_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = """
        SELECT COUNT(*) AS cnt
        FROM mypcp_roadvant.tbl_nationgard AS r
        """
        return _fetch_scalar(session, sql, {})

    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_nationgard AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- TWS ---------------------------------------------------------------------


def tws_coverage_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          CASE
            WHEN r.ContractStatus = 'A' THEN 'Active'
            WHEN r.ContractStatus = 'E' THEN 'Matured'
            WHEN r.ContractStatus = 'C' THEN 'Cancelled'
            ELSE 'In-Active'
          END AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.SaleDate),'%m/%d/%Y') AS SaleDate,
          r.ContractNumber AS ContractNo,
          r.LastName AS CustomerLName,
          r.FirstName AS CUST_FIRST_NAME,
          r.LastName  AS CUST_LAST_NAME,
          r.VIN,
          r.ContractNumber AS PolicyNumber,
          r.CoverageName AS COVERAGE_NAME,
          r.ProductType AS COV_TYPE,
          r.ContractStatus AS COV_STATUS,
          r.SaleDate AS PurchaseDate,
          r.SaleDate AS EFF_DATE,
          r.TermMileage AS EFF_MILEAGE,
          'N/A' AS EXP_DATE,
          'N/A' AS EXP_MILEAGE,
          r.DeductibleAmount AS DEDUCTIBLE
        FROM mypcp_roadvant.tbl_tws AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.FirstName AS CUST_FIRST_NAME,
      r.LastName  AS CUST_LAST_NAME,
      r.VIN,
      r.ContractNumber AS PolicyNumber,
      r.CoverageName  AS COVERAGE_NAME,
      r.ProductType   AS COV_TYPE,
      r.ContractStatus AS COV_STATUS,
      r.SaleDate AS PurchaseDate,
      r.SaleDate AS EFF_DATE,
      r.TermMileage AS EFF_MILEAGE,
      'N/A' AS EXP_DATE,
      'N/A' AS EXP_MILEAGE,
      r.DeductibleAmount AS DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_tws AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def tws_coverage_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_tws AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_tws AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- Assurant ----------------------------------------------------------------


def assurant_coverage_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          r.ContractStatus AS Status,
          DATE_FORMAT((r.SaleDate),'%m/%d/%Y') AS SaleDate,
          r.ContractNumber AS ContractNo,
          r.LastName AS CustomerLName,
          r.FirstName AS CUST_FIRST_NAME,
          r.LastName  AS CUST_LAST_NAME,
          r.VIN,
          r.ContractNumber AS PolicyNumber,
          r.CoverageName AS COVERAGE_NAME,
          r.ProductType AS COV_TYPE,
          r.ContractStatus AS COV_STATUS,
          r.SaleDate AS PurchaseDate,
          r.SaleDate AS EFF_DATE,
          r.TermMileage AS EFF_MILEAGE,
          'N/A' AS EXP_DATE,
          'N/A' AS EXP_MILEAGE,
          r.DeductibleAmount AS DEDUCTIBLE
        FROM mypcp_roadvant.tbl_assurant AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    # Note: CI version only joins tbl_customer when filters present — we follow that.
    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.FirstName AS CUST_FIRST_NAME,
      r.LastName  AS CUST_LAST_NAME,
      r.VIN,
      r.ContractNumber AS PolicyNumber,
      r.CoverageName AS COVERAGE_NAME,
      r.ProductType AS COV_TYPE,
      r.ContractStatus AS COV_STATUS,
      r.SaleDate AS PurchaseDate,
      r.SaleDate AS EFF_DATE,
      r.TermMileage AS EFF_MILEAGE,
      'N/A' AS EXP_DATE,
      'N/A' AS EXP_MILEAGE,
      r.DeductibleAmount AS DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_assurant AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def assurant_coverage_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_assurant AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_assurant AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- CareGuard ----------------------------------------------------------------


def careguard_coverage_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          CASE
            WHEN r.ContractStatus = 'A' THEN 'Active'
            WHEN r.ContractStatus = 'E' THEN 'Matured'
            WHEN r.ContractStatus = 'C' THEN 'Cancelled'
            ELSE 'In-Active'
          END AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.SaleDate),'%m/%d/%Y') AS SaleDate,
          r.ContractNumber AS ContractNo,
          r.LastName AS CustomerLName,
          r.FirstName AS CUST_FIRST_NAME,
          r.LastName  AS CUST_LAST_NAME,
          r.VIN,
          r.ContractNumber AS PolicyNumber,
          r.CoverageName AS COVERAGE_NAME,
          r.ProductType AS COV_TYPE,
          r.ContractStatus AS COV_STATUS,
          r.SaleDate AS PurchaseDate,
          r.SaleDate AS EFF_DATE,
          r.TermMileage AS EFF_MILEAGE,
          'N/A' AS EXP_DATE,
          'N/A' AS EXP_MILEAGE,
          r.DeductibleAmount AS DEDUCTIBLE
        FROM mypcp_roadvant.tbl_careguard AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.FirstName AS CUST_FIRST_NAME,
      r.LastName  AS CUST_LAST_NAME,
      r.VIN,
      r.ContractNumber AS PolicyNumber,
      r.CoverageName  AS COVERAGE_NAME,
      r.ProductType   AS COV_TYPE,
      r.ContractStatus AS COV_STATUS,
      r.SaleDate AS PurchaseDate,
      r.SaleDate AS EFF_DATE,
      r.TermMileage AS EFF_MILEAGE,
      'N/A' AS EXP_DATE,
      'N/A' AS EXP_MILEAGE,
      r.DeductibleAmount AS DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_careguard AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def careguard_coverage_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_careguard AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mycp_risk.mypcp_roadvant.tbl_careguard AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.LastName)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    # Note: The original had the same schema; I’ve kept to mypcp_roadvant.* — fix schema if different.
    sql = sql.replace("mycp_risk.", "")  # safety in case of copy typo
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- CARS --------------------------------------------------------------------


def cars_coverage_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          CASE
            WHEN r.CoverageStatus = 'A' THEN 'Active'
            WHEN r.CoverageStatus = 'E' THEN 'Matured'
            WHEN r.CoverageStatus = 'C' THEN 'Cancelled'
            ELSE 'In-Active'
          END AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.PurchaseDate),'%m/%d/%Y') AS SaleDate,
          r.PolicyNumber AS ContractNo,
          r.CustomerLName,
          r.CustomerFName AS CUST_FIRST_NAME,
          r.CustomerLName AS CUST_LAST_NAME,
          r.VIN,
          r.PolicyNumber,
          r.CoverageDescription,
          r.CoverageDescription AS COVERAGE_NAME,
          r.CoverageType AS COV_TYPE,
          r.CoverageStatus AS COV_STATUS,
          r.PurchaseDate,
          r.PurchaseDate AS EFF_DATE,
          r.ProductMileage AS EFF_MILEAGE,
          'N/A' AS EXP_DATE,
          'N/A' AS EXP_MILEAGE,
          r.Deductible AS DEDUCTIBLE
        FROM mypcp_roadvant.tbl_cars_coverage AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.CustomerFName AS CUST_FIRST_NAME,
      r.CoverageDescription,
      r.CustomerLName AS CUST_LAST_NAME,
      r.VIN,
      r.PolicyNumber,
      r.CoverageDescription AS COVERAGE_NAME,
      r.CoverageType AS COV_TYPE,
      r.CoverageStatus AS COV_STATUS,
      r.PurchaseDate,
      r.PurchaseDate AS EFF_DATE,
      r.ProductMileage AS EFF_MILEAGE,
      'N/A' AS EXP_DATE,
      'N/A' AS EXP_MILEAGE,
      r.Deductible AS DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_cars_coverage AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CustomerLName)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def cars_coverage_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_cars_coverage AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_cars_coverage AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CustomerLName)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- Amynta Warranty Solution -------------------------------------------------


def amynta_warranty_solution(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          r.COV_STATUS AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.PURCHASE_DATE),'%m/%d/%Y') AS SaleDate,
          r.CONTRACT_NUM AS ContractNo,
          r.CUST_LAST_NAME AS CustomerLName,
          r.CUST_FIRST_NAME,
          r.CUST_LAST_NAME,
          r.VIN,
          r.CONTRACT_NUM,
          r.COV_TYPE,
          r.COV_STATUS,
          r.COVERAGE_NAME,
          r.PURCHASE_DATE,
          r.EFF_DATE,
          r.EFF_MILEAGE,
          r.EXP_DATE,
          r.EXP_MILEAGE,
          r.DEDUCTIBLE
        FROM mypcp_roadvant.tbl_amynta_warranty_solution AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.CUST_FIRST_NAME,
      r.CUST_LAST_NAME,
      r.VIN,
      r.CONTRACT_NUM,
      r.COVERAGE_NAME,
      r.COV_TYPE,
      r.COV_STATUS,
      r.PURCHASE_DATE,
      r.EFF_DATE,
      r.EFF_MILEAGE,
      r.EXP_DATE,
      r.EXP_MILEAGE,
      r.DEDUCTIBLE
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_amynta_warranty_solution AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CUST_LAST_NAME)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def amynta_warranty_solution_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_amynta_warranty_solution AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_amynta_warranty_solution AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CUST_LAST_NAME)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- Smart AutoCare -----------------------------------------------------------


def smart_autocare(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          r.ContractStatus AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.EfectiveDate),'%m/%d/%Y') AS SaleDate,
          r.PolicyNumber AS ContractNo,
          r.LastName AS CustomerLName,
          r.VIN,
          r.CustomerName,
          r.ProductLineType,
          r.Term,
          r.ExpirationOdometer,
          r.Product,
          r.EfectiveDate,
          r.ExpirationDate,
          r.PolicyNumber,
          r.CancelDate,
          r.ContractPaidDate,
          r.DealerName,
          r.DealerNumber
        FROM smartautocare.tbl_smart_autocare AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})

    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.VIN,
      r.CustomerName,
      r.ProductLineType,
      r.Term,
      r.ExpirationOdometer,
      r.Product,
      r.EfectiveDate,
      r.ExpirationDate,
      r.PolicyNumber,
      r.CancelDate,
      r.ContractPaidDate,
      r.DealerName,
      r.DealerNumber
    FROM tbl_contract AS t
    JOIN smartautocare.tbl_smart_autocare AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CustomerName)) LIKE LOWER(CONCAT('%', TRIM(:LastName), '%'))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def smart_autocare_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM smartautocare.tbl_smart_autocare AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN smartautocare.tbl_smart_autocare AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.CustomerName)) LIKE LOWER(CONCAT('%', TRIM(:LastName), '%'))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- RoadVant -----------------------------------------------------------------


def roadvant(
    session: Session,
    DealerID: int,
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    if VIN == "" and LastName == "":
        sql = f"""
        SELECT DISTINCT
          r.coverage_type AS Status,
          DATE_FORMAT(FROM_UNIXTIME(r.purchase_date),'%m/%d/%Y') AS SaleDate,
          r.contract_number AS ContractNo,
          r.last_name AS CustomerLName,
          r.VIN,
          r.first_name,
          r.last_name,
          r.contract_number,
          r.coverage_type,
          r.coverage_name,
          r.purchase_date,
          r.product_term,
          r.product_milage,
          r.expiration_milage,
          r.expiration_date,
          r.dealer_name, r.dealer_number
        FROM mypcp_roadvant.tbl_roadvant AS r
        ORDER BY SaleDate DESC
        { _maybe_limit_offset(limit, offset) }
        """
        return _fetch_all_dicts(session, sql, {})
    sql = f"""
    SELECT DISTINCT
      CASE
        WHEN t.Status = 'I' THEN 'In-Active'
        WHEN t.Status = 'L' THEN 'Active'
        WHEN t.Status = 'M' THEN 'Matured'
        WHEN t.Status = 'C' THEN 'Cancelled'
        WHEN t.Status = 'S' THEN 'Perpetual'
        WHEN t.Status = 'P' THEN 'Pending Matured'
        ELSE 'In-Active'
      END AS Status,
      DATE_FORMAT(FROM_UNIXTIME(t.SaleDate),'%m/%d/%Y') AS SaleDate,
      t.ContractNo,
      c.CustomerLName,
      r.id,
      r.dealer_number,
      r.dealer_name,
      r.contract_number,
      r.VIN,
      r.first_name,
      r.last_name,
      r.coverage_type,
      r.coverage_name,
      r.purchase_date,
      r.product_term,
      r.product_milage,
      r.expiration_milage,
      r.expiration_date
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_roadvant AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
    { "AND t.VIN = :VIN" if VIN else "" }
    { "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.last_name)) = LOWER(TRIM(:LastName))" if LastName else "" }
    ORDER BY SaleDate DESC
    { _maybe_limit_offset(limit, offset) }
    """
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_all_dicts(session, sql, params)


def roadvant_count(
    session: Session, DealerID: int, VIN: str = "", LastName: str = ""
) -> int:
    if VIN == "" and LastName == "":
        sql = "SELECT COUNT(*) AS cnt FROM mypcp_roadvant.tbl_roadvant AS r"
        return _fetch_scalar(session, sql, {})
    sql = """
    SELECT COUNT(DISTINCT t.ContractNo) AS cnt
    FROM tbl_contract AS t
    JOIN mypcp_roadvant.tbl_roadvant AS r ON t.VIN = r.VIN
    JOIN tbl_customer AS c ON c.CustomerID = t.CustomerID
    WHERE t.DealerID = :DealerID
      {vin_clause}
      {name_clause}
    """.format(
        vin_clause="AND t.VIN = :VIN" if VIN else "",
        name_clause=(
            "AND LOWER(TRIM(c.CustomerLName)) = LOWER(TRIM(:LastName)) AND LOWER(TRIM(r.last_name)) = LOWER(TRIM(:LastName))"
            if LastName
            else ""
        ),
    )
    params = {"DealerID": int(DealerID)}
    if VIN:
        params["VIN"] = VIN
    if LastName:
        params["LastName"] = LastName
    return _fetch_scalar(session, sql, params)


# --- Export Router (mirrors PHP export()) ------------------------------------


def export(
    CoverageName: int = 1,
    DealerID: int = "2975",
    VIN: str = "",
    LastName: str = "",
    limit: Union[int, str] = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    with Session(engine) as session:
        """
        Mirrors the PHP:
        - If no VIN and no LastName:
            return raw table rows (no dealer filter), optionally limited.
        - Else:
            route to the corresponding coverage_* function with DealerID and filters.
        CoverageName mapping:
            1 = roadvant
            2 = smartautocare
            3 = amynta_warranty_solution
            4 = cars_coverage_solution
            5 = careguard
            6 = nationguard
            7 = tws
            8 = assurant
        """
        if VIN == "" and LastName == "":
            lo = _maybe_limit_offset(limit, offset)
            print("lo", lo)
            table_sql = {
                1: f"SELECT * FROM mypcp_roadvant.tbl_roadvant {lo}",
                2: f"SELECT * FROM smartautocare.tbl_smart_autocare {lo}",
                3: f"SELECT * FROM mypcp_roadvant.tbl_amynta_warranty_solution {lo}",
                4: f"SELECT * FROM mypcp_roadvant.tbl_cars_coverage {lo}",
                5: f"SELECT * FROM mypcp_roadvant.tbl_careguard {lo}",
                6: f"SELECT * FROM mycp_risk.mypcp_roadvant.tbl_nationgard {lo}".replace(
                    "mycp_risk.", ""
                ),
                7: f"SELECT * FROM mypcp_roadvant.tbl_tws {lo}",
                8: f"SELECT * FROM mypcp_roadvant.tbl_assurant {lo}",
            }
            sql = table_sql.get(int(CoverageName))
            if not sql:
                return []
            return _fetch_all_dicts(session, sql, {})

        # With filters: route
        data = None

        if CoverageName == 1:
            return roadvant(session, DealerID, VIN, LastName, limit, offset)

        elif CoverageName == 2:
            return smart_autocare(session, DealerID, VIN, LastName, limit, offset)
        elif CoverageName == 3:
            return amynta_warranty_solution(
                session, DealerID, VIN, LastName, limit, offset
            )
        elif CoverageName == 4:
            return cars_coverage_solution(
                session, DealerID, VIN, LastName, limit, offset
            )
        elif CoverageName == 5:
            return careguard_coverage_solution(
                session, DealerID, VIN, LastName, limit, offset
            )
        elif CoverageName == 6:
            return nationguard_coverage_solution(
                session, DealerID, VIN, LastName, limit, offset
            )
        elif CoverageName == 7:
            return tws_coverage_solution(
                session, DealerID, VIN, LastName, limit, offset
            )
        elif CoverageName == 8:
            return assurant_coverage_solution(
                session, DealerID, VIN, LastName, limit, offset
            )

    return []


# --- Example usage (optional) -------------------------------------------------
# from ftpcoverages import export
# with Session(engine) as s:
#     rows = export(s, CoverageName=1, DealerID=123, VIN='', LastName='', limit=100, offset=0)
#     print(len(rows), "rows")
