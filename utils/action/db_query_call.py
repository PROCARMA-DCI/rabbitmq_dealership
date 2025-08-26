from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from openpyxl import Workbook
from typing import List
from sqlmodel import create_engine, Session, text
from config import (
    DATABASE_URL,
    BASE_URL,
)
from utils.helpers import formatDate
from utils.consumer_utils import send_email

# ✅ Setup engine
DATABASE_URL = DATABASE_URL
engine = create_engine(DATABASE_URL, echo=True)


# ✅ Function to run raw query
def get_contract_details(contract_id: int):
    print("===================get_contract_details contract_id: ", contract_id)
    query = text(
        """
        SELECT DATE_FORMAT(FROM_UNIXTIME(tbl_contract.SaleDate),'%m/%d/%Y') AS SaleDate, IF(tbl_contract.UnlimitedTerm=1,'N/A',DATE_FORMAT(FROM_UNIXTIME(tbl_contract.ValidityDate),'%m/%d/%Y')) AS ValidityDate,tbl_contract.VIN,tbl_contract.ContractID,tbl_contract.ContractNo,
tbl_customer.CustomerFName,tbl_customer.CustomerLName,tbl_customer.PrimaryEmail,tbl_customer.PhoneHome,
tbl_planmaster.PlanDescription,tbl_planmaster.PlanID,tbl_planmaster.PlanCode,tbl_planmaster.ValidityDays,tbl_planmaster.ValidityMileage,tbl_dealer.DealerID,tbl_dealer.DealerTitle,
tbl_dealer.ContPersonPhone,tbl_dealer.CityName,(SELECT StateTitle FROM tbl_states WHERE tbl_dealer.StateID=tbl_states.StateID) AS State,
(SELECT country.iso3 FROM country WHERE country.id=tbl_dealer.DealerCountry) AS Country,tbl_dealer.DealerAddress1,tbl_dealer.DealerAddress2,
tbl_dealer.DealerZIP,tbl_dealer.ContPersonEmail,tbl_dealer.ContPerson,
(SELECT SUM(tbl_contractcoupon.CouponValue) FROM tbl_contractcoupon WHERE tbl_contractcoupon.ContractID=tbl_contract.ContractID AND tbl_contractcoupon.CouponID IN('10880279', '10880287', '10880294')) AS CouponValue
FROM tbl_contract
JOIN tbl_customer ON(tbl_customer.CustomerID=tbl_contract.CustomerID)
JOIN tbl_planmaster ON(tbl_contract.PlanID=tbl_planmaster.PlanID)
JOIN tbl_dealer ON(tbl_dealer.DealerID=tbl_contract.DealerID)
WHERE tbl_contract.ContractID=:contract_id
    """
    )

    with Session(engine) as session:
        result = session.execute(query, {"contract_id": contract_id})
        rows = result.mappings().all()  # returns list of dict-like rows
        if not rows:
            return None  # no record

        if len(rows) == 1:
            return dict(rows[0])  # single row → dict

        return [dict(r) for r in rows]  # multiple rows → list of dicts


def get_coupons_details(contract_id: int, coupon_ids: List[int]):
    # Convert list into tuple for SQL IN clause
    coupon_ids_tuple = tuple(coupon_ids)

    query = text(
        f"""
        SELECT COUNT(CouponID) AS totalCoupon,
               CouponTitle,
               IF(VariablePrice>0,VariablePrice,CouponValue) AS CouponValue,
               RepairOrderNo,
               DATE_FORMAT(FROM_UNIXTIME(RecievedDate),'%m/%d/%Y') AS RecievedDate,
               CheckNo,
               CouponMileage,
               UserID,
               VariablePrice,
               ServiceAmounts,
               ServiceType,
               ServiceID,
               ModifiedDate
        FROM tbl_contractcoupon
        WHERE ContractID = :contract_id
          AND CouponID IN :coupon_ids
        GROUP BY CouponTitle, ServiceType, ServiceID
    """
    )

    with Session(engine) as session:
        result = session.execute(
            query, {"contract_id": contract_id, "coupon_ids": coupon_ids_tuple}
        )
        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]


def get_api_credentials(ID: int = 1):
    query = text(
        """
        SELECT 
            Notes,
            SandBoxUrl,
            LiveUrl,
            IsLive,
            UserName,
            tbl_api_dealerid.Password,
            SandboxUserName,
            SandboxPassword,
            RequestType
        FROM tbl_api_dealerid 
        WHERE ID = :ID
    """
    )

    with Session(engine) as session:
        result = session.execute(query, {"ID": ID})
        rows = result.mappings().all()  # returns list of dict-like rows
        if not rows:
            return None  # no record

        if len(rows) == 1:
            return dict(rows[0])  # single row → dict

        return [dict(r) for r in rows]  # multiple rows → list of dicts


def export_contracts(dealer_id):
    """Equivalent of ftpcoverages_mdl->export() in PHP"""
    query = text(
        """
        SELECT ContractID, DealerID, VIN, SaleDate
        FROM tbl_contract
        WHERE DealerID = :dealer_id
    """
    )

    with Session(engine) as session:
        result = session.execute(query, {"dealer_id": dealer_id})
        rows = result.mappings().all()
        return [dict(r) for r in rows]


def export_to_email(
    contractDetails: dict,
    ID: int = 23220,
    # DealerID: int = 2975,
    # VIN: str = "5TFJA5DBXPX062422",
    # LastName: str = "Procarma",
    # Email: str = "support@procarma.com",
    # CoverageName: int = 1,
):
    VIN = contractDetails.get("VIN")
    LastName = contractDetails.get("CustomerLName")
    Email = contractDetails.get("ContPersonEmail")
    DealerID = contractDetails.get("DealerID")

    with Session(engine) as session:

        if not Email:
            print("No pending email exports.")
            return

        dealer_id = DealerID
        pcp_user_id = 1
        matching = 0

        # if matching == 2:  # reset VIN & LastName
        #     vin, last_name = "", ""

        # 2. Fetch contracts
        contracts = export_contracts(dealer_id)
        contracts = [
            {
                **item,
                "VIN": VIN,
                "CustomerLName": LastName,
                "CustomerEmail": Email,
                "SaleDate": formatDate(item.get("SaleDate")),
            }
            for item in contracts
        ]

        if not contracts:
            print("No contracts found.")
            return

        # 3. Build Excel file
        wb = Workbook()
        ws = wb.active
        ws.append(list(contracts[0].keys()))  # header row
        for c in contracts:
            ws.append(list(c.values()))

        save_dir = "/var/www/html/testing/exports"
        os.makedirs(save_dir, exist_ok=True)

        filename = f"{dealer_id}-{pcp_user_id}-export_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.xlsx"
        file_path = os.path.join(save_dir, filename)
        wb.save(file_path)

        # Public URL (don’t include the local file path)
        public_url = f"{BASE_URL}/testing/exports/{filename}"

        # 4. Prepare email
        subject = "Coverage Export File"
        email_msg = f"""
        Hi,<br><br>
        Your data is available on this link
        <a href="{public_url}" target="_blank">Download</a>.<br><br>
        Please download from here. This will be retained on server for 7 days.<br><br>
        Thanks.<br>
        PROCARMA Team
        """

        if send_email(Email, subject, email_msg):
            print("✅ Email sent successfully")
            # 5. Update record in DB
            result = session.execute(
                text(
                    """
                UPDATE mypcp_roadvant.tbl_emailexport
                SET SendEmail=1, EmailSentDate=:date
                WHERE ID=:id
                """
                ),
                {
                    "date": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "id": ID,
                },
            )

            print(
                f"🔎 Rows updated: {result.rowcount}"
            )  # check how many rows were updated

            session.commit()
        else:
            print("❌ Email not sent successfully")
