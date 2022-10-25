import io
import time
from zipfile import ZipFile

import pandas as pd
import requests

import gridstatus
from gridstatus import utils
from gridstatus.base import FuelMix, GridStatus, ISOBase, Markets
from gridstatus.decorators import support_date_range


class CAISO(ISOBase):
    BASE = "https://www.caiso.com/outlook/SP"
    HISTORY_BASE = "https://www.caiso.com/outlook/SP/History"

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    status_homepage = "https://www.caiso.com/TodaysOutlook/Pages/default.aspx"

    # Markets PRC_RTPD_LMP, PRC_HASP_LMP, PRC_LMP
    markets = [
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    trading_hub_locations = [
        "TH_NP15_GEN-APND",
        "TH_SP15_GEN-APND",
        "TH_ZP26_GEN-APND",
    ]

    def _current_day(self):
        # get current date from stats api
        return self.get_latest_status().time.date()

    def get_stats(self):
        stats_url = self.BASE + "/stats.txt"
        r = self._get_json(stats_url)
        return r

    def get_latest_status(self) -> str:
        """Get Current Status of the Grid

        Known possible values: Normal, Restricted Maintenance Operations, Flex Alert
        """

        # todo is it possible for this to return more than one element?
        r = self.get_stats()

        time = pd.to_datetime(r["slotDate"]).tz_localize("US/Pacific")
        # can only store one value for status so we concat them together
        status = ", ".join(r["gridstatus"])
        reserves = r["Current_reserve"]

        return GridStatus(time=time, status=status, reserves=reserves, iso=self)

    def get_latest_fuel_mix(self):
        """
        Returns most recent data point for fuelmix in MW

        Updates every 5 minutes
        """
        url = self.BASE + "/fuelsource.csv"
        df = pd.read_csv(url)

        mix = df.iloc[-1].to_dict()
        time = _make_timestamp(mix.pop("Time"), self._current_day())

        return FuelMix(time=time, mix=mix, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel_mix for today in 5 minute intervals"
        # todo should this use the latest endpoint?
        return self._today_from_historical(self.get_historical_fuel_mix)

    @support_date_range(frequency="1D")
    def get_historical_fuel_mix(self, date, verbose=False):
        """
        Get historical fuel mix in 5 minute intervals for a provided day

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623

        Returns:
            dataframe

        """
        url = self.HISTORY_BASE + "/%s/fuelsource.csv"
        df = _get_historical(url, date, verbose=verbose)

        # rename some inconsistent columns names to standardize across dates
        df = df.rename(
            columns={
                "Small hydro": "Small Hydro",
                "Natural gas": "Natural Gas",
                "Large hydro": "Large Hydro",
            },
        )

        # when day light savings time switches, there are na rows
        df = df.dropna()

        return df

    def get_latest_load(self):
        """Returns most recent data point for load in MW

        Updates every 5 minutes
        """
        load_url = self.BASE + "/demand.csv"
        df = pd.read_csv(load_url)

        # get last non null row
        data = df[~df["Current demand"].isnull()].iloc[-1]

        return {
            "time": _make_timestamp(data["Time"], self._current_day()),
            "load": data["Current demand"],
        }

    def get_load_today(self):
        "Get load for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_load)

    @support_date_range(frequency="1D")
    def get_historical_load(self, date, verbose=False):
        """Return load at a previous date in 5 minute intervals"""
        url = self.HISTORY_BASE + "/%s/demand.csv"
        df = _get_historical(url, date, verbose=verbose)[["Time", "Current demand"]]
        df = df.rename(columns={"Current demand": "Demand"})
        df = df.dropna(subset=["Demand"])

        return df

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        return self._latest_supply_from_fuel_mix()

    def get_supply_today(self):
        "Get supply for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in 5 minute intervals"""
        return self._supply_from_fuel_mix(date)

    def get_forecast_today(self):
        """Get load forecast for today in 1 hour intervals"""
        d = self._today_from_historical(self.get_historical_forecast)
        return d

    @support_date_range(frequency="31D")
    def get_historical_forecast(self, date, end=None, sleep=5, verbose=False):
        """Returns load forecast for a previous date in 1 hour intervals

        Arguments:
            date(datetime, pd.Timestamp, or str): day to return. if string, format should be YYYYMMDD e.g 20200623
            sleep (int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds."""
        start, end = _caiso_handle_start_end(date, end)

        url = (
            "http://oasis.caiso.com/oasisapi/SingleZip?"
            + "resultformat=6&queryname=SLD_FCST&version=1&market_run_id=DAM"
            + f"&startdatetime={start}&enddatetime={end}"
        )

        df = _get_oasis(
            url,
            usecols=["INTERVALSTARTTIME_GMT", "MW", "TAC_AREA_NAME"],
            verbose=verbose,
        ).rename(
            columns={"INTERVALSTARTTIME_GMT": "Time", "MW": "Load Forecast"},
        )

        # returns many areas, we only want one overall iso
        df = df[df["TAC_AREA_NAME"] == "CA ISO-TAC"]

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)
        df = df.sort_values("Time")

        df["Forecast Time"] = df["Time"].iloc[0]

        df = df[["Forecast Time", "Time", "Load Forecast"]]
        time.sleep(sleep)
        return df

    def get_pnodes(self):
        url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_PNODE_MAP&version=1&startdatetime=20220801T07:00-0000&enddatetime=20220802T07:00-0000&pnode_id=ALL"
        df = pd.read_csv(
            url,
            compression="zip",
            usecols=["APNODE_ID", "PNODE_ID"],
        ).rename(
            columns={
                "APNODE_ID": "Aggregate PNode ID",
                "PNODE_ID": "PNode ID",
            },
        )
        return df

    def get_latest_lmp(self, market: str, locations: list = None):
        return self._latest_lmp_from_today(market=market, locations=locations)

    def get_lmp_today(self, market: str, locations: list = None):
        "Get lmp for today in 5 minute intervals"
        return self._today_from_historical(
            self.get_historical_lmp,
            market=market,
            locations=locations,
        )

    @support_date_range(frequency="31D")
    def get_historical_lmp(
        self,
        date,
        market: str,
        locations: list = None,
        sleep: int = 5,
        end=None,
        verbose=False,
    ):
        """Get day ahead LMP pricing starting at supplied date for a list of locations.

        Arguments:
            date: date to return data

            market: market to return from. supports:

            locations (list): list of locations to get data from. If no locations are provided, defaults to NP15, SP15, and ZP26, which are the trading hub locations. For a list of locations, call CAISO.get_pnodes()

            sleep (int): number of seconds to sleep before returning to avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        Returns
            dataframe of pricing data
        """

        if locations is None:
            locations = self.trading_hub_locations

        # todo make sure defaults to local timezone
        start, end = _caiso_handle_start_end(date, end)

        market = Markets(market)
        if market == Markets.DAY_AHEAD_HOURLY:
            query_name = "PRC_LMP"
            market_run_id = "DAM"
            version = 12
            PRICE_COL = "MW"
        elif market == Markets.REAL_TIME_15_MIN:
            query_name = "PRC_RTPD_LMP"
            market_run_id = "RTPD"
            version = 3
            PRICE_COL = "PRC"
        elif market == Markets.REAL_TIME_HOURLY:
            query_name = "PRC_HASP_LMP"
            market_run_id = "HASP"
            version = 3
            PRICE_COL = "MW"
        else:
            raise RuntimeError("LMP Market is not supported")

        nodes_str = ",".join(locations)
        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname={query_name}&version={version}&startdatetime={start}&enddatetime={end}&market_run_id={market_run_id}&node={nodes_str}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "NODE",
                "LMP_TYPE",
                PRICE_COL,
            ],
            verbose=verbose,
        )

        df = df.pivot_table(
            index=["INTERVALSTARTTIME_GMT", "NODE"],
            columns="LMP_TYPE",
            values=PRICE_COL,
            aggfunc="first",
        )

        df = df.reset_index().rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
            },
        )

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

        df["Market"] = market.value
        df["Location Type"] = None

        df.loc[
            df["Location"].isin(self.trading_hub_locations),
            "Location Type",
        ] = "Trading Hub"

        df = df[
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        data = utils.filter_lmp_locations(df, locations)

        time.sleep(sleep)

        return df

    def get_storage_today(self):
        """Return storage charging or discharging for today in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date: date to return data
        """
        return self._today_from_historical(self.get_historical_storage)

    @support_date_range(frequency="1D")
    def get_historical_storage(self, date, verbose=False):
        """Return storage charging or discharging at a previous date in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date: date to return data
        """
        url = self.HISTORY_BASE + "/%s/storage.csv"
        df = _get_historical(url, date, verbose=verbose)
        df = df.rename(columns={"Batteries": "Supply"})
        df["Type"] = "Batteries"
        return df

    @support_date_range(frequency="31D")
    def get_historical_gas_prices(
        self,
        date,
        end=None,
        fuel_region_id="ALL",
        sleep=5,
        verbose=False,
    ):
        """Return gas prices at a previous date

        Arguments:
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
            fuel_region_id (str, or list): single fuel region id or list of fuel region ids to return data for. Defaults to ALL, which returns all fuel regions.
        """

        start, end = _caiso_handle_start_end(date, end)

        if isinstance(fuel_region_id, list):
            fuel_region_id = ",".join(fuel_region_id)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_FUEL&version=1&FUEL_REGION_ID={fuel_region_id}&startdatetime={start}&enddatetime={end}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "FUEL_REGION_ID",
                "PRC",
            ],
            verbose=verbose,
        ).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "FUEL_REGION_ID": "Fuel Region Id",
                "PRC": "Price",
            },
        )
        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)
        df = (
            df.sort_values("Time")
            .sort_values(
                ["Fuel Region Id", "Time"],
            )
            .reset_index(drop=True)
        )
        time.sleep(sleep)
        return df

    @support_date_range(frequency="31D")
    def get_historical_ghg_allowance(
        self,
        date,
        end=None,
        sleep=5,
        verbose=False,
    ):
        """Return ghg allowance at a previous date

        Arguments:
            date: date to return data
            end: last date of range to return data. if None, returns only date. Defaults to None.
        """

        start, end = _caiso_handle_start_end(date, end)

        url = f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_GHG_ALLOWANCE&version=1&startdatetime={start}&enddatetime={end}"

        df = _get_oasis(
            url,
            usecols=[
                "INTERVALSTARTTIME_GMT",
                "GHG_PRC_IDX",
            ],
            verbose=verbose,
        ).rename(
            columns={
                "INTERVALSTARTTIME_GMT": "Time",
                "GHG_PRC_IDX": "GHG Allowance Price",
            },
        )

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

        time.sleep(sleep)
        return df


def _make_timestamp(time_str, today, timezone="US/Pacific"):
    hour, minute = map(int, time_str.split(":"))
    return pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
        tz=timezone,
    )


def _get_historical(url, date, verbose=False):
    date_str = date.strftime("%Y%m%d")
    date_obj = date
    url = url % date_str
    df = pd.read_csv(url)

    if verbose:
        print("Fetching URL: ", url)

    df["Time"] = df["Time"].apply(
        _make_timestamp,
        today=date_obj,
        timezone="US/Pacific",
    )

    # sometimes returns midnight, which is technically the next day
    # to be careful, let's check if that is the case before dropping
    if df.iloc[-1]["Time"].hour == 0:
        df = df.iloc[:-1]

    return df


def _get_oasis(url, usecols=None, verbose=False):
    if verbose:
        print(url)

    retry_num = 0
    while retry_num < 3:
        r = requests.get(url)

        if r.status_code == 200:
            break

        retry_num += 1
        print(f"Failed to get data from CAISO. Error: {r.status_code}")
        print(f"Retrying {retry_num}...")
        time.sleep(5)

    z = ZipFile(io.BytesIO(r.content))

    df = pd.read_csv(
        z.open(z.namelist()[0]),
        usecols=usecols,
    )

    return df


def _caiso_handle_start_end(date, end):
    start = date.tz_convert("UTC")

    if end:
        end = end
        end = end.tz_convert("UTC")
    else:
        end = start + pd.DateOffset(1)

    start = start.strftime("%Y%m%dT%H:%M-0000")
    end = end.strftime("%Y%m%dT%H:%M-0000")

    return start, end


if __name__ == "__main__":
    import gridstatus

    print("asd")
    iso = gridstatus.CAISO()
    df = iso.get_historical_lmp(
        "feb 1, 2020",
        "DAY_AHEAD_HOURLY",
        locations=["TH_NP15_GEN-APND"],
    )
