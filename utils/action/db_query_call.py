import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import os
from openpyxl import Workbook
import smtplib
from typing import List
from sqlmodel import create_engine, Session, text
from config import (
    DATABASE_URL,
    SENDGRID_SMTP,
    SENDGRID_USER,
    SENDGRID_PASS,
    BASE_URL,
    DEFAULT_FROM_EMAIL,
)
from utils.helpers import Print


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


def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """Send email via SendGrid SMTP"""
    try:
        msg = MIMEMultipart()
        msg["From"] = DEFAULT_FROM_EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(SENDGRID_SMTP, 587) as server:
            server.starttls()
            server.login(SENDGRID_USER, SENDGRID_PASS)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"❌ Email sending failed: {e}")
        return False


def export_contracts(coverage_name, dealer_id, vin=None, last_name=None):
    """Equivalent of ftpcoverages_mdl->export() in PHP"""
    query = text(
        """
        SELECT ContractID, DealerID, VIN, CustomerLName, CoverageName, SaleDate
        FROM tbl_contract
        WHERE DealerID = :dealer_id
    """
    )

    with Session(engine) as session:
        result = session.execute(query, {"dealer_id": dealer_id})
        Print(result, "result=====================")
        rows = result.mappings().all()
        return [dict(r) for r in rows]


def export_to_email():
    rows = {}
    with Session(engine) as session:
        # 1. Fetch all records where SendEmail = 0
        rows = [
            {
                "ID": 23188,
                "Email": "support@procarma.com",
                "VIN": "5TFJA5DBXPX062422",
                "DealerID": 2975,
                "LastName": "Procarma",
                "CoverageName": 1,
            }
        ]
        if not rows:
            print("No pending email exports.")
            return

        for row in rows:
            email = row["Email"]
            if not email or "@" not in email:
                continue

            coverage_name = row.get("CoverageName")
            dealer_id = row.get("DealerID")
            vin = row.get("VIN", "")
            last_name = row.get("LastName", "")
            pcp_user_id = row.get("pcp_user_id", "1")
            matching = row.get("Matching", 0)

            if matching == 2:  # reset VIN & LastName
                vin, last_name = "", ""

            # 2. Fetch contracts
            contracts = export_contracts(coverage_name, dealer_id, vin, last_name)
            if not contracts:
                continue

            # 3. Build Excel file
            wb = Workbook()
            ws = wb.active
            ws.append(list(contracts[0].keys()))  # header row
            for c in contracts:
                ws.append(list(c.values()))

            filename = f"exports/{dealer_id}-{pcp_user_id}-export_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            wb.save(filename)

            # 4. Prepare email
            subject = "Coverage Export File"
            email_msg = f"""
            Hi,<br><br>
            Your data is available on this link 
            <a href="{BASE_URL}{filename}" target="_blank">Download</a>.<br><br>
            Please download from here. This will be retained on server for 7 days.<br><br>
            Thanks.<br>
            PROCARMA Team
            """

            print(f"{email} | Following Message Sent By Customer:\n{email_msg}")

            if send_email(email, subject, email_msg):
                print("✅ Email sent successfully")
                # 5. Update record in DB
                session.exec(
                    text(
                        """
                        UPDATE mypcp_roadvant.tbl_emailexport
                        SET SendEmail=1, EmailSentDate=:date
                        WHERE ID=:id
                    """
                    ),
                    {
                        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "id": row["ID"],
                    },
                )
                session.commit()
            else:
                print("❌ Email not sent successfully")
