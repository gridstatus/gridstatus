from bdb import set_trace

import pandas as pd

from isodata import utils
from isodata.base import FuelMix, GridStatus, ISOBase


class Ercot(ISOBase):
    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"
    default_timezone = "US/Central"

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"

    def get_latest_status(self):
        r = self._get_json(self.BASE + "/daily-prc.json")

        time = (
            pd.to_datetime(r["current_condition"]["datetime"], unit="s")
            .tz_localize("UTC")
            .tz_convert(self.default_timezone)
        )
        status = r["current_condition"]["state"]
        reserves = float(r["current_condition"]["prc_value"].replace(",", ""))

        if status == "normal":
            status = "Normal"

        return GridStatus(time=time, status=status, reserves=reserves, iso=self.name)

    def get_latest_fuel_mix(self):
        df = self.get_fuel_mix_today()
        currentHour = df.iloc[-1]

        mix_dict = {"Wind": currentHour["Wind"], "Solar": currentHour["Solar"]}

        return FuelMix(time=currentHour["Time"], mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        """Get historical fuel mix

        Only supports current day
        """
        url = self.BASE + "/combine-wind-solar.json"
        r = self._get_json(url)

        # rows with nulls are forecasts
        df = pd.DataFrame(r["currentDay"]["data"])
        df = df.dropna(subset=["actualSolar"])

        df = self._handle_data(
            df,
            {"actualSolar": "Solar", "actualWind": "Wind"},
        )
        return df

    def get_latest_demand(self):
        d = self._get_demand("currentDay").iloc[-1]

        return {"time": d["Time"], "demand": d["Demand"]}

    def _get_demand(self, when):
        """Returns demand for currentDay or previousDay"""
        # todo switch to https://www.ercot.com/content/cdr/html/20220810_actual_loads_of_forecast_zones.html
        # says supports last 5 days, appears to support last two weeks
        # df = pd.read_html("https://www.ercot.com/content/cdr/html/20220810_actual_loads_of_forecast_zones.html")
        # even more historical data. up to month back i think: https://www.ercot.com/mp/data-products/data-product-details?id=NP6-346-CD
        # ^ confusing to figure out how to parse though. use this endpoint: https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=14836&_=1660840704920
        url = self.BASE + "/loadForecastVsActual.json"
        r = self._get_json(url)
        df = pd.DataFrame(r[when]["data"])
        df = df.dropna(subset=["systemLoad"])
        df = self._handle_data(df, {"systemLoad": "Demand"})
        return df

    def get_demand_today(self):
        """Returns demand for today"""
        return self._get_demand("currentDay")

    def get_latest_supply(self):
        return self._latest_from_today(self.get_supply_today)

    def get_supply_today(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
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
        ).dt.tz_localize(self.default_timezone)

        data = data[data["forecast"] == 0]  # only keep non forecast rows

        data = data[["Time", "capacity"]].rename(
            columns={"capacity": "Supply"},
        )

        return data

    def get_forecast_today(self):
        # intrahour https://www.ercot.com/mp/data-products/data-product-details?id=NP3-562-CD
        # there are a few days of historical date for the forecast
        today = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc, publish_date = self._get_document(
            report_type_id=12311,
            date=today,
        )

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone)

        doc = doc.rename(columns={"SystemTotal": "Load Forecast"})
        doc["Forecast Time"] = publish_date

        doc = doc[["Forecast Time", "Time", "Load Forecast"]]

        return doc

    def _get_document(self, report_type_id, date):
        """Get document for a given report type id and date. If multiple document published return the latest"""
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        match = []
        for d in docs:
            if "csv" not in d["Document"]["FriendlyName"]:
                continue

            doc_date = pd.Timestamp(d["Document"]["PublishDate"]).tz_convert(
                self.default_timezone,
            )

            # check do we need to check if same timezone?
            if doc_date.date() == date.date():
                match.append((doc_date, d["Document"]["DocID"]))

        if len(match) == 0:
            raise ValueError(
                f"No document found for {report_type_id} on {date}",
            )

        doc = max(match, key=lambda x: x[0])
        csv_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc[1]}"
        return pd.read_csv(csv_url, compression="zip"), doc[0]

    def _handle_data(self, df, columns):
        df["Time"] = (
            pd.to_datetime(df["epoch"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.default_timezone)
        )

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)
