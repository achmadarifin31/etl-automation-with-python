# (1) Import packages

# (a) external packages
import pandas as pd

# (b) internal packages
from utils.google_utils import (
    remove_disallowed_characters,
    load_to_bigquery,
    read_from_Google_sheet,
)
import constants

###############################################################

# (2) Set run parameters

gcp_dataset = "offline_sales"
refresh_date = pd.Timestamp(pd.Timestamp.now(), tz="Asia/Singapore")

###############################################################

# (3) Internal functions

def main(
    google_sheet_id,
    google_sheet_tab_name,
    project_cy_code,
    project_dept_code,
    project_env_code,
    dataset,
    table,
    load_type,
    upload_date,
):
    df = read_from_Google_sheet(
        google_sheet_id=google_sheet_id, google_sheet_tab_name=google_sheet_tab_name
    )
    df = remove_disallowed_characters(df)
    project = (
        "project-gcp-" + project_cy_code + "-" + project_dept_code + "-" + project_env_code
    )
    load_to_bigquery(
        df=df,
        project=project,
        dataset=dataset,
        table=table,
        load_type=load_type,
        upload_date=upload_date,
    )

###############################################################

# (4) Main code

if __name__ == "__main__":

    # Sell Offline
    main(
        google_sheet_id="link sheet",
        google_sheet_tab_name="name sheet",
        project_cy_code=constants.gcp_country_code,
        project_dept_code=constants.gcp_dept_code,
        project_env_code="prod",
        dataset=gcp_dataset,
        table="tbl_orders_offline",
        load_type="overwrite",
        upload_date=refresh_date,
    )

