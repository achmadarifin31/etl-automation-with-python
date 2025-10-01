def post_process_Offline_Sales_Gideon(df):
    """Convert columns for Offline Sales (GIDEON) dataset to CDL column names"""

    # (1) Order-level columns
    df.rename(columns={"No Faktur": "OrID"}, inplace=True)
    df.rename(columns={"Tgl Faktur": "OrDatetime"}, inplace=True)
    df["Salesman_name"] = ""
    df["op_cancelled"] = False
    df["cz_province"] = "Region_1"
    df["wh_name"] = "WH1"
    df["OrStatus"] = "SELL"

    # Normalize outlet names
    df["Nama Outlet"] = (
        df["Nama Outlet"]
        .astype(str)
        .str.replace("／", "/", regex=False)
        .str.replace("\u00a0", " ", regex=False)
        .str.strip()
    )

    # Initialize cz_name from outlet name
    df["cz_name"] = df["Nama Outlet"].astype(str).str.strip()

    # If column "Jns Outlet" exists, use it as cz_type.
    # If missing, split outlet name into cz_name / cz_type
    if "Jns Outlet" in df.columns:
        df["cz_type"] = df["Jns Outlet"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})

        # For rows where cz_type is missing, derive from outlet name
        mask_missing = df["cz_type"].isna()
        if mask_missing.any():
            df.loc[mask_missing, ["cz_name", "cz_type"]] = (
                df.loc[mask_missing, "Nama Outlet"]
                .astype(str)
                .str.split("/", n=1, expand=True)
                .values
            )
    else:
        # If "Jns Outlet" does not exist at all, derive everything from outlet name
        df[["cz_name", "cz_type"]] = df["Nama Outlet"].astype(str).str.split("/", n=1, expand=True)

    # Final cleanup: strip whitespaces & normalize empty values to NA
    df["cz_name"] = df["cz_name"].astype(str).str.strip()
    df["cz_type"] = df["cz_type"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})

    # Map outlet type abbreviations to full names
    outlet_type_mapping = {
        "APTK": "APOTEK",
        "IB": "IBU",
        "CV": "CV",
        "NN": "NONA",
        "PT": "PT",
        "TK": "TOKO",
        "TKOBT": "TOKO OBAT",
        "TN": "TUAN",
    }
    df["cz_type"] = df["cz_type"].str.upper()
    df["cz_type"] = df["cz_type"].str.replace(r"^M\s*/\s*", "", regex=True)
    df["cz_type"] = df["cz_type"].str.replace(r"^M$", "", regex=True)
    for short, full in outlet_type_mapping.items():
        df["cz_type"] = df["cz_type"].str.replace(rf"\b{short}\b", full, regex=True)

    # (2) Product Identifiers
    df["OpPrSKU"] = "Unknown"
    df.rename(columns={"Satuan": "OpPrSKU_Variation"}, inplace=True)
    df.rename(columns={"Nama Barang": "OpPrName"}, inplace=True)

    # Cleanup whitespace in product fields
    df["OpPrSKU_Variation"] = df["OpPrSKU_Variation"].astype(str).str.strip()
    df["OpPrName"] = df["OpPrName"].astype(str).str.strip()

    # Merge product mapping using name + variation
    df = df.merge(
        default_keyword_product_mapping,
        on=["OpPrName", "OpPrSKU_Variation"],
        how="left",
    )

    # (3) GIDEON Specialty Retailer
    df["Retailer"] = "GT"
    df["Retailer_Segment"] = "GT"

    # (4) Quantity and Price
    df.rename(columns={"QTY": "OpQty"}, inplace=True)
    df.rename(columns={"HNA": "Op_price_original"}, inplace=True)
    df.rename(columns={"Total": "Op_topline_original"}, inplace=True)
    df.rename(columns={"Total_Amount": "Op_topline_discounted"}, inplace=True)
    df = convert_column_type_to_numeric(df, "OpQty", int)

    # Ensure numeric formatting for price-related columns
    for i in ("Op_price_original", "Op_topline_original", "Op_topline_discounted"):
        if df[i].dtype == "object":
            df[i] = (
                df[i]
                .astype(str)
                .str.replace("(", "-", regex=False)
                .str.replace(")", "", regex=False)
                .str.replace("–", "-", regex=False)
                .str.replace(".", ".", regex=False)
                .str.replace(",", "", regex=False)
            )
            df[i] = pd.to_numeric(df[i], errors="coerce")
        else:
            df[i] = df[i].astype(float)

    df["OpPricingDiscount"] = df["Potongan"].astype(float)
    df["Op_price_discounted"] = df["Op_topline_discounted"] / df["OpQty"]
    df["OpVoucherCode"] = "Unknown"
    df["OpVoucherDiscount"] = 0
    df["cz_uses_voucher"] = 0

    # (5) Returned flag
    df["op_returned"] = df["Op_topline_discounted"] < 0
    df.loc[df["op_returned"], "OrStatus"] = "RETURN"

    # (6) Origin/destination
    returned = df["op_returned"] == True
    not_returned = df["op_returned"] == False

    # Non-returned orders
    df.loc[not_returned, "origin_name"] = "WH1"
    df.loc[not_returned, "origin_type"] = "DIST"
    df.loc[not_returned, "destination_name"] = df.loc[not_returned, "cz_name"]
    df.loc[not_returned, "destination_type"] = "RT"

    # Returned orders
    df.loc[returned, "origin_name"] = df.loc[returned, "cz_name"]
    df.loc[returned, "origin_type"] = "RT"
    df.loc[returned, "destination_name"] = "WH1"
    df.loc[returned, "destination_type"] = "DIST"

    df["to_Rupiah_conversion"] = 1

    return df
