import pandas as pd

from gridstatus import utils
from gridstatus.base import (
    FuelMix,
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    NotSupported,
)
from gridstatus.decorators import support_date_range


class Ercot(ISOBase):
    """Electric Reliability Council of Texas (ERCOT)"""

    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"
    default_timezone = "US/Central"

    status_homepage = "https://www.ercot.com/gridmktinfo/dashboards/gridconditions"
    interconnection_homepage = (
        "http://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933"
    )

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"

    def get_status(self, date, verbose=False):
        """Returns status of grid"""
        if date != "latest":
            raise NotSupported()

        r = self._get_json(self.BASE + "/daily-prc.json", verbose=verbose)

        time = (
            pd.to_datetime(r["current_condition"]["datetime"], unit="s")
            .tz_localize("UTC")
            .tz_convert(self.default_timezone)
        )
        status = r["current_condition"]["state"]
        reserves = float(r["current_condition"]["prc_value"].replace(",", ""))

        if status == "normal":
            status = "Normal"

        notes = [r["current_condition"]["condition_note"]]

        return GridStatus(
            time=time,
            status=status,
            reserves=reserves,
            iso=self,
            notes=notes,
        )

    def get_fuel_mix(self, date, verbose=False):
        """Get fuel mix in hourly intervals. Currently, ercot only reports solar and wind generation

        Arguments:
            date(datetime or str): "latest", "today". historical data currently not supported

            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pd.Dataframe: dataframe with columns: Time and columns for each fuel type (solar and wind)
        """

        if date == "latest":
            df = self.get_fuel_mix("today")
            currentHour = df.iloc[-1]

            mix_dict = {
                "Solar": currentHour["Solar"],
                "Wind": currentHour["Wind"],
                "Other": currentHour["Other"],
            }

            return FuelMix(time=currentHour["Time"], mix=mix_dict, iso=self.name)

        elif utils.is_today(date):

            url = self.BASE + "/combine-wind-solar.json"
            r = self._get_json(url, verbose=verbose)

            # rows with nulls are forecasts
            df = pd.DataFrame(r["currentDay"]["data"].values())
            df = df.dropna(subset=["actualSolar"])

            df = self._handle_data(
                df,
                {"actualSolar": "Solar", "actualWind": "Wind"},
            )
            supply_df = self._get_supply("today")
            mix = df.merge(supply_df, on="Time").rename({"Supply": "Other"})
            mix["Other"] = mix["Supply"] - mix["Solar"] - mix["Wind"]

            return mix[["Time", "Solar", "Wind", "Other"]]

        else:
            raise NotSupported()

    def get_load(self, date, verbose=False):
        if date == "latest":
            d = self._get_load("currentDay").iloc[-1]

            return {"time": d["Time"], "load": d["Load"]}

        elif utils.is_today(date):
            return self._get_load("currentDay")

        else:
            raise NotSupported()

    def _get_load(self, when):
        """Returns load for currentDay or previousDay"""
        # todo switch to https://www.ercot.com/content/cdr/html/20220810_actual_loads_of_forecast_zones.html
        # says supports last 5 days, appears to support last two weeks
        # df = pd.read_html("https://www.ercot.com/content/cdr/html/20220810_actual_loads_of_forecast_zones.html")
        # even more historical data. up to month back i think: https://www.ercot.com/mp/data-products/data-product-details?id=NP6-346-CD
        # hourly load archives: https://www.ercot.com/gridinfo/load/load_hist
        url = self.BASE + "/loadForecastVsActual.json"
        r = self._get_json(url)
        df = pd.DataFrame(r[when]["data"])
        df = df.dropna(subset=["systemLoad"])
        df = self._handle_data(df, {"systemLoad": "Load"})
        return df

    def _get_supply(self, date, verbose=False):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        assert date == "today", "Only today's data is supported"
        url = "https://www.ercot.com/api/1/services/read/dashboards/todays-outlook.json"
        r = self._get_json(url)

        date = pd.to_datetime(r["lastUpdated"][:10], format="%Y-%m-%d")

        # ignore last row since that corresponds to midnight following day
        data = pd.DataFrame(r["data"][:-1])

        data["Time"] = pd.to_datetime(
            date.strftime("%Y-%m-%d")
            + " "
            + data["hourEnding"].astype(str).str.zfill(2)
            + ":"
            + data["interval"].astype(str).str.zfill(2),
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        data = data[data["forecast"] == 0]  # only keep non forecast rows

        data = data[["Time", "capacity"]].rename(
            columns={"capacity": "Supply"},
        )

        return data

    def get_load_forecast(self, date, verbose=False):
        """Returns load forecast

        Currently only supports today's forecast
        """
        if date != "today":
            raise NotSupported()

        # intrahour https://www.ercot.com/mp/data-products/data-product-details?id=NP3-562-CD
        # there are a few days of historical date for the forecast
        today = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc_url, publish_date = self._get_document(
            report_type_id=12311,
            date=today,
            constructed_name_contains="csv.zip",
        )

        doc = pd.read_csv(doc_url, compression="zip")

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        doc = doc.rename(columns={"SystemTotal": "Load Forecast"})
        doc["Forecast Time"] = publish_date

        doc = doc[["Forecast Time", "Time", "Load Forecast"]]

        return doc

    @support_date_range("1D")
    def get_as_prices(self, date, verbose=False):
        """Get ancillary service clearing prices in hourly intervals in Day Ahead Market

        Arguments:
            date(datetime or str): date of delivery for AS services
            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pd.Dataframe: dataframe with prices for "Non-Spinning Reserves", "Regulation Up", "Regulation Down", "Responsive Reserves",

        """
        # subtract one day since it's the day ahead market happens on the day before for the delivery day
        date = date - pd.Timedelta("1D")

        report_type_id = 12329
        doc_url, date = self._get_document(
            report_type_id,
            date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        if verbose:
            print("Downloading {}".format(doc_url))

        doc = pd.read_csv(doc_url, compression="zip")

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous=doc["DSTFlag"] == "Y")

        doc["Market"] = "DAM"

        # NSPIN  REGDN  REGUP    RRS
        rename = {
            "NSPIN": "Non-Spinning Reserves",
            "REGDN": "Regulation Down",
            "REGUP": "Regulation Up",
            "RRS": "Responsive Reserves",
        }
        data = (
            doc.pivot_table(
                index=["Time", "Market"],
                columns="AncillaryType",
                values="MCPC",
            )
            .rename(columns=rename)
            .reset_index()
        )

        data.columns.name = None

        return data

    def get_rtm_spp(self, year):
        """Get Historical RTM Settlement Point Prices(SPPs) for each of the Hubs and Load Zones

        Arguments:
            year(int): year to get data for

        Source: https: // www.ercot.com/mp/data-products/data-product-details?id = NP6-785-ER
        """
        doc_url, date = self._get_document(
            13061,
            constructed_name_contains=f"{year}.zip",
            verbose=True,
        )

        x = utils.get_zip_file(doc_url)
        all_sheets = pd.read_excel(x, sheet_name=None)
        df = pd.concat(all_sheets.values())
        return df

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue for ERCOT

        Monthly historical data available here: http: // mis.ercot.com/misapp/GetReports.do?reportTypeId = 15933 & reportTitle = GIS % 20Report & showHTMLView = &mimicKey
        """

        report_type_id = 15933
        doc_url, date = self._get_document(
            report_type_id=report_type_id,
            constructed_name_contains="GIS_Report",
        )

        # TODO other sheets for small projects, inactive, and cancelled project
        # TODO see if this data matches up with summaries in excel file
        # TODO historical data available as well

        if verbose:
            print("Downloading interconnection queue from: ", doc_url)

        # skip rows and handle header
        queue = pd.read_excel(
            doc_url,
            sheet_name="Project Details - Large Gen",
            skiprows=30,
        ).iloc[4:]

        queue["State"] = "Texas"
        queue["Queue Date"] = queue["Screening Study Started"]

        fuel_type_map = {
            "BIO": "Biomass",
            "COA": "Coal",
            "GAS": "Gas",
            "GEO": "Geothermal",
            "HYD": "Hydrogen",
            "NUC": "Nuclear",
            "OIL": "Fuel Oil",
            "OTH": "Other",
            "PET": "Petcoke",
            "SOL": "Solar",
            "WAT": "Water",
            "WIN": "Wind",
        }

        technology_type_map = {
            "BA": "Battery Energy Storage",
            "CC": "Combined-Cycle",
            "CE": "Compressed Air Energy Storage",
            "CP": "Concentrated Solar Power",
            "EN": "Energy Storage",
            "FC": "Fuel Cell",
            "GT": "Combustion (gas) Turbine, but not part of a Combined-Cycle",
            "HY": "Hydroelectric Turbine",
            "IC": "Internal Combustion Engine, eg. Reciprocating",
            "OT": "Other",
            "PV": "Photovoltaic Solar",
            "ST": "Steam Turbine other than Combined-Cycle",
            "WT": "Wind Turbine",
        }

        queue["Fuel"] = queue["Fuel"].map(fuel_type_map)
        queue["Technology"] = queue["Technology"].map(technology_type_map)

        queue["Generation Type"] = queue["Fuel"] + " - " + queue["Technology"]

        queue["Status"] = (
            queue["IA Signed"]
            .isna()
            .map(
                {
                    True: InterconnectionQueueStatus.ACTIVE.value,
                    False: InterconnectionQueueStatus.COMPLETED.value,
                },
            )
        )

        queue["Actual Completion Date"] = queue["Approved for Synchronization"]

        rename = {
            "INR": "Queue ID",
            "Project Name": "Project Name",
            "Interconnecting Entity": "Interconnecting Entity",
            "Projected COD": "Proposed Completion Date",
            "POI Location": "Interconnection Location",
            "County": "County",
            "State": "State",
            "Capacity (MW)": "Capacity (MW)",
            "Queue Date": "Queue Date",
            "Generation Type": "Generation Type",
            "Actual Completion Date": "Actual Completion Date",
            "Status": "Status",
        }

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
        extra_columns = [
            "Fuel",
            "Technology",
            "GIM Study Phase",
            "Screening Study Started",
            "Screening Study Complete",
            "FIS Requested",
            "FIS Approved",
            "Economic Study Required",
            "IA Signed",
            "Air Permit",
            "GHG Permit",
            "Water Availability",
            "Meets Planning",
            "Meets All Planning",
            "CDR Reporting Zone",
            # "Construction Start", # all null
            # "Construction End", # all null
            "Approved for Energization",
            "Approved for Synchronization",
            "Comment",
        ]

        missing = [
            # todo the actual complettion date can be calculated by looking at status and other date columns
            "Withdrawal Comment",
            "Transmission Owner",
            "Summer Capacity (MW)",
            "Winter Capacity (MW)",
            "Withdrawn Date",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    def _get_document(
        self,
        report_type_id,
        date=None,
        constructed_name_contains=None,
        verbose=False,
    ):
        """Get document for a given report type id and date. If multiple document published return the latest"""
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        match = []
        for d in docs:
            doc_date = pd.Timestamp(d["Document"]["PublishDate"]).tz_convert(
                self.default_timezone,
            )

            # check do we need to check if same timezone?
            if date and doc_date.date() != date.date():
                continue

            if (
                constructed_name_contains
                and constructed_name_contains not in d["Document"]["ConstructedName"]
            ):
                continue

            match.append((doc_date, d["Document"]["DocID"]))

        if len(match) == 0:
            raise ValueError(
                f"No document found for {report_type_id} on {date}",
            )

        doc = max(match, key=lambda x: x[0])
        url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc[1]}"
        return url, doc[0]

    def _handle_data(self, df, columns):
        df["Time"] = (
            pd.to_datetime(df["epoch"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.default_timezone)
        )

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)


if __name__ == "__main__":
    iso = Ercot()
    iso.get_fuel_mix("today")
