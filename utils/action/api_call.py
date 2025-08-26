import datetime
import requests

from utils.helpers import Print


def request_AUTTO(contractDetails: dict, coupansDetails: dict, apiCredentials: dict):
    username = apiCredentials.get("SandboxUserName")
    password = apiCredentials.get("SandboxPassword")
    ContractNo = contractDetails.get("ContractNo")
    domain = "https://first-secured-staging.herokuapp.com"
    url = f"{domain}/api/v2/contracts/{ContractNo}/claims"
    mapped = [
        {
            "title": item["CouponTitle"],
            "name": item["CouponTitle"],  # coupan code
            "labor_time": 0,
            "shop_labor_rate": 0,
            "is_covered": False,
            "covered_labor_rate": 0,
            "covered_labor_time": 0,
        }
        for item in coupansDetails
    ]
    parts = []

    if contractDetails.get("DealerAddress1"):
        parts.append(contractDetails["DealerAddress1"])

    if contractDetails.get("CityName"):
        parts.append(f"City: {contractDetails['CityName']}")

    if contractDetails.get("StateName"):
        parts.append(f"State: {contractDetails['StateName']}")

    if contractDetails.get("DealerZIP"):
        parts.append(f"Zip: {contractDetails['DealerZIP']}")

    if contractDetails.get("Country"):
        parts.append(f"Country: {contractDetails['Country']}")

    address = ", ".join(parts)
    data = {
        "cost_in_cents": float(contractDetails.get("CouponValue") or 0) * 100,
        "odometer": max(item["CouponMileage"] for item in coupansDetails),
        "shop_name": contractDetails.get("ContPerson"),
        "shop_phone": contractDetails.get("ContPersonPhone"),
        "shop_address": address,
        "shop_rep": contractDetails.get("DealerTitle"),
        "shop_email": contractDetails.get("ContPersonEmail"),
        "shop_notes": apiCredentials.get("Notes"),
        "approved_at": datetime.date.today().strftime("%m/%d/%Y"),
        "repaired_at": datetime.date.today().strftime("%m/%d/%Y"),
        "claim_components_attributes": mapped,
    }
    Print(data, "data")
    try:
        print("🔗 URL:", url)
        print("👤 Username:", username, "password:", password)
        response = requests.post(url, json=data, auth=(username, password))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None


def request_SOAP(apiCredentials: dict, contractDetails: dict, coupansDetails: dict):
    url = "https://services.ase-profittrack.com/ClaimService.asmx"
    username = apiCredentials.get("SandboxUserName")
    password = apiCredentials.get("SandboxPassword")

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": url,
    }

    data = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
             <InsertClaim xmlns="http://services.ase-profittrack.com/">
             <InsertClaimRequest>
                 <System_ID>MYPCP123</System_ID>
                 <User_ID>{username}</User_ID>
                <User_Password>{password}</User_Password>
                <Contract_ContractNumber>{contractDetails.get("ContractNo")}</Contract_ContractNumber>
                 <Contract_VIN>{contractDetails.get("VIN")}</Contract_VIN>
                <Contract_DealerNumber>{contractDetails.get("DealerID")}</Contract_DealerNumber>
                <Contract_EffectiveDate>{contractDetails.get("SaleDate")}</Contract_EffectiveDate>
                <Contract_ProductLineCode>{contractDetails.get("PlanID")}</Contract_ProductLineCode>
                <Contract_PlanCode>{contractDetails.get("PlanID")}</Contract_PlanCode>
                <Contract_Customer_LastName>{contractDetails.get("CustomerLName")}</Contract_Customer_LastName>
                <Claim_Number>{contractDetails.get("ContractNo")}</Claim_Number>
                <Claim_LossDate>{contractDetails.get("ValidityDate")}</Claim_LossDate>
                <Claim_RepairOrderNumber>RO2024001</Claim_RepairOrderNumber>
                <Claim_LossOdometer>{max(item["CouponMileage"] for item in coupansDetails)}</Claim_LossOdometer>
                <Claim_Complaint>{contractDetails.get("CouponValue")}</Claim_Complaint>
                <Claim_Cause></Claim_Cause>
                <Claim_CorrectiveAction></Claim_CorrectiveAction>
                <Claim_EstimatedRepairCost>{max(item["RepairOrderNo"] for item in coupansDetails)}</Claim_EstimatedRepairCost>
                <Claim_Payment_ReferenceNumber>{max(item["RepairOrderNo"] for item in coupansDetails)}</Claim_Payment_ReferenceNumber>
                <Claim_Payment_CheckNumber>{coupansDetails[0].get("CheckNo")}</Claim_Payment_CheckNumber>
                <Claim_Payment_CheckPrintDate>{datetime.date.today().strftime("%m/%d/%Y")}</Claim_Payment_CheckPrintDate>
                <Claim_AllowMultipleClaimsInPast7Days>false</Claim_AllowMultipleClaimsInPast7Days>
                <Claim_ApplyDeductible>true</Claim_ApplyDeductible>
                <Claim_RoadSideSO></Claim_RoadSideSO>
                 <Shop_EntityNumber>SHOP001</Shop_EntityNumber>
                  <Claim_PartLines>
        
                 {  "".join(
                        f"""
                        <ClaimImportPartLine>
                            <Claim_Part_ItemName>{item.get("CouponTitle")}</Claim_Part_ItemName>
                            <Claim_Part_ItemDescription>{item.get("CouponTitle")}</Claim_Part_ItemDescription>
                            <Claim_Part_RequestedQuantity>{item.get("totalCoupon")}</Claim_Part_RequestedQuantity>
                            <Claim_Part_RequestedUnitPrice>{item.get("CouponValue")}</Claim_Part_RequestedUnitPrice>
                            <Claim_Part_AuthorizedQuantity>0</Claim_Part_AuthorizedQuantity>
                            <Claim_Part_AuthorizedUnitPrice>0</Claim_Part_AuthorizedUnitPrice>
                            <Claim_Part_TaxRate>0</Claim_Part_TaxRate>
                            <Claim_Part_TaxAmount>0</Claim_Part_TaxAmount>
                        </ClaimImportPartLine>
                        """
                        for item in coupansDetails
                    )
                }
              
                   </Claim_PartLines>
              </InsertClaimRequest>
              </InsertClaim>
            </soap:Body>
            </soap:Envelope>"""

    print("📦 SOAP Body:\n", data)

    try:
        response = requests.post(
            url,
            data=data.encode("utf-8"),
            headers=headers,
        )
        response.raise_for_status()
        return response.text  # SOAP returns XML string
    except requests.exceptions.RequestException as e:
        print(f"SOAP request failed: {e}")
        return None
