from ast import Raise

import pandas as pd
import requests

import isodata
from isodata.base import FuelMix, ISOBase, Markets


class PJM(ISOBase):
    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    def get_latest_fuel_mix(self):
        mix = self.get_fuel_mix_today()
        latest = mix.iloc[-1]
        time = latest.pop("Time")
        mix_dict = latest.to_dict()
        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel mix for today in hourly intervals"
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        date = date = isodata.utils._handle_date(date)
        tomorrow = date + pd.DateOffset(1)

        data = {
            "datetime_beginning_ept": date.strftime("%m/%d/%Y 00:00")
            + "to"
            + tomorrow.strftime("%m/%d/%Y 00:00"),
            "fields": "datetime_beginning_ept,fuel_type,is_renewable,mw",
            "rowCount": 1000,
            "startRow": 1,
        }

        r = self._get_pjm_json("gen_by_fuel", params=data)

        mix_df = pd.DataFrame(r["items"])

        mix_df = mix_df.pivot_table(
            index="datetime_beginning_ept",
            columns="fuel_type",
            values="mw",
            aggfunc="first",
        ).reset_index()

        mix_df["datetime_beginning_ept"] = pd.to_datetime(
            mix_df["datetime_beginning_ept"],
        ).dt.tz_localize(self.default_timezone)

        mix_df = mix_df.rename(columns={"datetime_beginning_ept": "Time"})

        return mix_df

    def get_latest_supply(self):
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in hourly intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date at hourly intervals"""
        return self._supply_from_fuel_mix(date)

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        """Returns demand at a previous date at 5 minute intervals

        Args:
            date (str or datetime.date): date to get demand for. must be in last 30 days
        """
        # todo can support a load area
        date = isodata.utils._handle_date(date)
        tomorrow = date + pd.DateOffset(1)

        data = {
            "datetime_beginning_ept": date.strftime("%m/%d/%Y 00:00")
            + "to"
            + tomorrow.strftime("%m/%d/%Y 00:00"),
            "sort": "datetime_beginning_utc",
            "order": "Asc",
            "startRow": 1,
            "isActiveMetadata": "true",
            "fields": "area,datetime_beginning_ept,instantaneous_load",
            "area": "PJM RTO",
            "format": "json",
            "download": "true",
        }
        r = self._get_pjm_json("inst_load", params=data)

        data = pd.DataFrame(r)

        demand = demand = data.drop("area", axis=1)

        demand = demand.rename(
            columns={
                "datetime_beginning_ept": "Time",
                "instantaneous_load": "Demand",
            },
        )

        demand["Time"] = pd.to_datetime(demand["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        demand = demand.sort_values("Time").reset_index(drop=True)
        return demand

    def get_forecast_today(self):
        """Get forecast for today in hourly intervals.

        Updates every Every half hour on the quarter E.g. 1:15 and 1:45

        """
        # todo: should we use the UTC field instead of EPT?
        data = {
            "startRow": 1,
            "rowCount": 1000,
            "fields": "evaluated_at_datetime_ept,forecast_area,forecast_datetime_beginning_ept,forecast_load_mw",
            "forecast_area": "RTO_COMBINED",
        }
        r = self._get_pjm_json("load_frcstd_7_day", params=data)
        data = pd.DataFrame(r["items"]).rename(
            columns={
                "evaluated_at_datetime_ept": "Forecast Time",
                "forecast_datetime_beginning_ept": "Time",
                "forecast_load_mw": "Load Forecast",
            },
        )

        data.drop("forecast_area", axis=1, inplace=True)

        data["Forecast Time"] = pd.to_datetime(data["Forecast Time"]).dt.tz_localize(
            self.default_timezone,
        )
        data["Time"] = pd.to_datetime(data["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        return data

    # todo https://dataminer2.pjm.com/feed/load_frcstd_hist/definition
    # def get_historical_forecast(self, date):
    # pass

    def get_pnode_ids(self):
        data = {
            "startRow": 1,
            "rowCount": 500000,
            "fields": "effective_date,pnode_id,pnode_name,pnode_subtype,pnode_type,termination_date,voltage_level,zone",
        }
        r = self._get_pjm_json("pnode", params=data)

        nodes = pd.DataFrame(r["items"])

        # only keep most recent effective date for each id
        # return sorted by pnode_id
        nodes = (
            nodes.sort_values("effective_date", ascending=False)
            .drop_duplicates(
                "pnode_id",
            )
            .sort_values("pnode_id")
            .reset_index(drop=True)
        )
        return nodes

    def get_latest_lmp(self, market: str, locations: list = None):
        """Currently only supports DAY_AHEAD_HOURlY"""
        market = Markets(market)
        if market != Markets.DAY_AHEAD_HOURLY:
            raise NotImplementedError("Only supports DAY_AHEAD_HOURLY")
        return self._latest_lmp_from_today(market, locations)

    def get_lmp_today(self, market: str, locations: list = None):
        """Get lmp for today
        Currently only supports DAY_AHEAD_HOURlY
        """
        # TODO try to find a different source of data for real time
        market = Markets(market)
        if market != Markets.DAY_AHEAD_HOURLY:
            raise NotImplementedError("Only supports DAY_AHEAD_HOURLY")
        return self._today_from_historical(self.get_historical_lmp, market, locations)

    def get_historical_lmp(self, date, market: str, locations: list = None):
        """Returns LMP at a previous date

        Args:
            date (str or datetime.date): date to get LMPs for
            market (str):  Supported Markets: REAL_TIME_5_MIN, REAL_TIME_HOURLY, DAY_AHEAD_HOURLY
            locations (list, optional):  list of pnodeid to get LMPs for. Defaults to Hubs. Use get_pnode_ids() to get a list of possible pnode ids

        """
        date = date = isodata.utils._handle_date(date)
        tomorrow = date + pd.DateOffset(1)

        if locations is None:
            locations = [
                "51217",
                "116013751",
                "35010337",
                "34497151",
                "34497127",
                "34497125",
                "33092315",
                "33092313",
                "33092311",
                "4669664",
                "51288",
                "51287",
            ]

        market = Markets(market)
        if market == Markets.REAL_TIME_5_MIN:
            market_endpoint = "rt_fivemin_hrl_lmps"
            market_type = "rt"
        elif market == Markets.REAL_TIME_HOURLY:
            market_endpoint = "rt_hrl_lmps"
            market_type = "rt"
        elif market == Markets.DAY_AHEAD_HOURLY:
            market_endpoint = "da_hrl_lmps"
            market_type = "da"
        else:
            raise ValueError(
                "market must be one of REAL_TIME_5_MIN, REAL_TIME_HOURLY, DAY_AHEAD_HOURLY",
            )

        #  TODO implement paging since row count can exceed 1000000
        params = {
            "datetime_beginning_ept": date.strftime("%m/%d/%Y 00:00")
            + "to"
            + tomorrow.strftime("%m/%d/%Y 00:00"),
            "startRow": 1,
            "rowCount": 1000000,
            "fields": f"congestion_price_{market_type},datetime_beginning_ept,datetime_beginning_utc,equipment,marginal_loss_price_{market_type},pnode_id,pnode_name,row_is_current,system_energy_price_{market_type},total_lmp_{market_type},type,version_nbr,voltage,zone",
            "pnode_id": ";".join(map(str, locations)),
        }

        r = self._get_pjm_json(market_endpoint, params=params)
        data = pd.DataFrame(r["items"]).rename(
            columns={
                "datetime_beginning_ept": "Time",
                "pnode_id": "Location",
                "pnode_name": "Location Name",
                "type": "Location Type",
                f"total_lmp_{market_type}": "LMP",
                f"system_energy_price_{market_type}": "Energy",
                f"congestion_price_{market_type}": "Congestion",
                f"marginal_loss_price_{market_type}": "Loss",
            },
        )

        data["Market"] = market.value

        data["Time"] = pd.to_datetime(data["Time"]).dt.tz_localize(
            self.default_timezone,
        )

        data = data[
            [
                "Time",
                "Market",
                "Location",
                "Location Name",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        return data

    def _get_pjm_json(self, endpoint, params):
        r = self._get_json(
            "https://api.pjm.com/api/v1/" + endpoint,
            params=params,
            headers={"Ocp-Apim-Subscription-Key": self._get_key()},
        )

        return r

    def _get_key(self):
        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json",
        )

        return settings["subscriptionKey"]


"""
import isodata
iso = isodata.PJM()
nodes = iso.get_pnode_ids()
zones = nodes[nodes["pnode_subtype"] == "ZONE"]
zone_ids = zones["pnode_id"].tolist()
iso.get_historical_lmp("Oct 1, 2022", "DAY_AHEAD_HOURLY", locations=zone_ids)
pnode_id
"""
