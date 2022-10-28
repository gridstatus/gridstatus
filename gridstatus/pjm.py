import math
import warnings

import pandas as pd
import tqdm

import gridstatus
from gridstatus import utils
from gridstatus.base import FuelMix, ISOBase, Markets, NotSupported
from gridstatus.decorators import (
    _get_pjm_archive_date,
    pjm_update_dates,
    support_date_range,
)


class PJM(ISOBase):
    """PJM"""

    name = "PJM"
    iso_id = "pjm"
    default_timezone = "US/Eastern"

    location_types = [
        "ZONE",
        "LOAD",
        "GEN",
        "AGGREGATE",
        "INTERFACE",
        "EXT",
        "HUB",
        "EHV",
        "TIE",
        "RESIDUAL_METERED_EDC",
    ]

    hub_node_ids = [
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

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]

    @support_date_range(frequency="365D")
    def get_fuel_mix(self, date, end=None, verbose=False):
        """Get fuel mix for a date or date range  in hourly intervals"""

        if date == "latest":
            mix = self.get_fuel_mix("today")
            latest = mix.iloc[-1]
            time = latest.pop("Time")
            mix_dict = latest.to_dict()
            return FuelMix(time=time, mix=mix_dict, iso=self.name)

        # earliest date available appears to be 1/1/2016
        data = {
            "fields": "datetime_beginning_utc,fuel_type,is_renewable,mw",
            "sort": "datetime_beginning_utc",
            "order": "Asc",
        }

        mix_df = self._get_pjm_json(
            "gen_by_fuel",
            start=date,
            end=end,
            params=data,
        )

        mix_df = mix_df.pivot_table(
            index="Time",
            columns="fuel_type",
            values="mw",
            aggfunc="first",
        ).reset_index()

        return mix_df

    @support_date_range(frequency="365D")
    def get_supply(self, date, end=None, verbose=False):
        """Get supply for a date or date range in hourly intervals"""
        return self._get_supply(date=date, end=end, verbose=verbose)

    @support_date_range(frequency="30D")
    def get_load(self, date, end=None):
        """Returns load at a previous date at 5 minute intervals

        Args:
            date (str or datetime.date): date to get load for. must be in last 30 days
        """

        if date == "latest":
            return self._latest_from_today(self.get_load)

        # more hourly historical load here: https://dataminer2.pjm.com/feed/hrl_load_metered/definition

        # todo can support a load area
        data = {
            "order": "Asc",
            "sort": "datetime_beginning_utc",
            "isActiveMetadata": "true",
            "fields": "area,datetime_beginning_utc,instantaneous_load",
            "area": "PJM RTO",
        }
        load = self._get_pjm_json(
            "inst_load",
            start=date,
            end=end,
            params=data,
        )

        load = load.drop("area", axis=1)

        load = load.rename(
            columns={
                "instantaneous_load": "Load",
            },
        )

        return load

    def get_load_forecast(self, date):
        """Get forecast for today in hourly intervals.

        Updates every Every half hour on the quarter E.g. 1:15 and 1:45

        """

        if date != "today":
            raise NotSupported()

        # todo: should we use the UTC field instead of EPT?
        params = {
            "fields": "evaluated_at_datetime_ept,forecast_area,forecast_datetime_beginning_ept,forecast_load_mw",
            "forecast_area": "RTO_COMBINED",
        }
        data = self._get_pjm_json(
            "load_frcstd_7_day",
            start=None,
            params=params,
        )
        data = data.rename(
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

        return data

    # todo https://dataminer2.pjm.com/feed/load_frcstd_hist/definition
    # def get_historical_forecast(self, date):
    # pass

    def get_pnode_ids(self):
        data = {
            "fields": "effective_date,pnode_id,pnode_name,pnode_subtype,pnode_type,termination_date,voltage_level,zone",
            "termination_date": "12/31/9999exact",
        }
        nodes = self._get_pjm_json("pnode", start=None, params=data)

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

    @support_date_range(frequency="365D", update_dates=pjm_update_dates)
    def get_lmp(
        self,
        date,
        market: str,
        end=None,
        locations="hubs",
        location_type=None,
        verbose=False,
    ):
        """Returns LMP at a previous date

         Notes:
            * If start date is prior to the PJM archive date, all data must be downloaded before location filtering can be performed due to limitations of PJM API. The archive date is
              186 days (~6 months) before today for the 5 minute real time market and 731 days (~2 years) before today for the Hourly Real Time and Day Ahead Hourly markets. Node type filter can
              performed for Real Time Hourly and Day Ahead Hourly markets.

            * If location_type is provided, it is filtered after data is retrieved for Real Time 5 Minute market regardless of the date. This is due to PJM api limitations

         Args:
             date (str or datetime.date): date to get LMPs for

             end (str or datetime.date): end date to get LMPs for

             market (str):  Supported Markets: REAL_TIME_5_MIN, REAL_TIME_HOURLY, DAY_AHEAD_HOURLY

             locations (list, optional):  list of pnodeid to get LMPs for. Defaults to "hubs". Use get_pnode_ids() to get a list of possible pnode ids.
             If "all", will return data from all p nodes (warning there are over 10,000 unique pnodes, so expect millions or billions of rows!)

             location_type (str, optional):  If specified, will only return data for nodes of this type. Defaults to None. Possible location types are: 'ZONE', 'LOAD', 'GEN', 'AGGREGATE', 'INTERFACE', 'EXT',
        'HUB', 'EHV', 'TIE', 'RESIDUAL_METERED_EDC'.

        """
        market = Markets(market)

        if date == "latest":
            """Currently only supports DAY_AHEAD_HOURlY"""
            if market != Markets.DAY_AHEAD_HOURLY:
                raise NotImplementedError("Only supports DAY_AHEAD_HOURLY")
            return self._latest_lmp_from_today(
                market=market,
                locations=locations,
                location_type=location_type,
                verbose=verbose,
            )

        elif utils.is_today(date):
            if market != Markets.DAY_AHEAD_HOURLY:
                raise NotImplementedError("Only supports DAY_AHEAD_HOURLY")

        if locations == "hubs":
            locations = self.hub_node_ids

        params = {}

        if market == Markets.REAL_TIME_5_MIN:
            market_endpoint = "rt_fivemin_hrl_lmps"
            market_type = "rt"
        elif market == Markets.REAL_TIME_HOURLY:
            # todo implemlement location type filter
            market_endpoint = "rt_hrl_lmps"
            market_type = "rt"
        elif market == Markets.DAY_AHEAD_HOURLY:
            # todo implemlement location type filter
            market_endpoint = "da_hrl_lmps"
            market_type = "da"
        else:
            raise ValueError(
                "market must be one of REAL_TIME_5_MIN, REAL_TIME_HOURLY, DAY_AHEAD_HOURLY",
            )

        if location_type:
            location_type = location_type.upper()
            if location_type not in self.location_types:
                raise ValueError(
                    f"location_type must be one of {self.location_types}",
                )

            if market == Markets.REAL_TIME_5_MIN:
                warnings.warn(
                    "When using Real Time 5 Minute market, location_type filter will happen after all data is downloaded",
                )
            else:
                params["type"] = f"*{location_type}*"

            if locations is not None:
                locations = None

        if date >= _get_pjm_archive_date(market):
            # after archive date, filtering allowed
            params["fields"] = (
                f"congestion_price_{market_type},datetime_beginning_ept,datetime_beginning_utc,equipment,marginal_loss_price_{market_type},pnode_id,pnode_name,row_is_current,system_energy_price_{market_type},total_lmp_{market_type},type,version_nbr,voltage,zone",
            )

            if locations and locations != "ALL":
                params["pnode_id"] = ";".join(map(str, locations))

        elif locations is not None:
            warnings.warn(
                "Querying before archive date, so filtering by location will happen after all data is downloaded",
            )

        data = self._get_pjm_json(
            market_endpoint,
            start=date,
            end=end,
            params=params,
            verbose=verbose,
        )

        data = data.rename(
            columns={
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

        # API cannot filter location type for rt 5 min
        if location_type and market == Markets.REAL_TIME_5_MIN:
            data = data[data["Location Type"] == location_type]

        if locations is not None and locations != "ALL":
            data = gridstatus.utils.filter_lmp_locations(
                data,
                map(int, locations),
            )

        return data

    def _get_pjm_json(
        self,
        endpoint,
        start,
        params,
        end=None,
        start_row=1,
        row_count=100000,
        verbose=False,
    ):
        default_params = {
            "startRow": start_row,
            "rowCount": row_count,
        }

        # update final params with default params
        final_params = params.copy()
        final_params.update(default_params)

        if start is not None:
            start = gridstatus.utils._handle_date(start)

            if end:
                end = gridstatus.utils._handle_date(end)
            else:
                end = start + pd.DateOffset(days=1)

            final_params["datetime_beginning_ept"] = (
                start.strftime("%m/%d/%Y %H:%M") + "to" + end.strftime("%m/%d/%Y %H:%M")
            )

        if verbose:
            print(
                f"Retrieving data from {endpoint} with params {final_params}",
            )

        api_key = self._get_key()
        r = self._get_json(
            "https://api.pjm.com/api/v1/" + endpoint,
            params=final_params,
            headers={"Ocp-Apim-Subscription-Key": api_key},
        )

        if "errors" in r:
            raise RuntimeError(r["errors"])

        # todo should this be a warning?
        if r["totalRows"] == 0:
            raise RuntimeError("No data found for query")

        df = pd.DataFrame(r["items"])

        num_pages = math.ceil(r["totalRows"] / row_count)
        if num_pages > 1:
            to_add = [df]
            for page in tqdm.tqdm(range(1, num_pages), initial=1, total=num_pages):
                next_url = [x for x in r["links"] if x["rel"] == "next"][0]["href"]
                r = self._get_json(
                    next_url,
                    headers={
                        "Ocp-Apim-Subscription-Key": api_key,
                    },
                )
                to_add.append(pd.DataFrame(r["items"]))

            df = pd.concat(to_add)

        if "datetime_beginning_utc" in df.columns:
            df["Time"] = (
                pd.to_datetime(df["datetime_beginning_utc"])
                .dt.tz_localize(
                    "UTC",
                )
                .dt.tz_convert(self.default_timezone)
            )

            # drop datetime_beginning_utc
            df = df.drop(columns=["datetime_beginning_utc"])

            # PJM API is inclusive of end, so we need to drop where end timestamp is included
            df = df[
                df["Time"].dt.strftime(
                    "%Y-%m-%d %H:%M",
                )
                != end.strftime("%Y-%m-%d %H:%M")
            ]

        return df

    def _get_key(self):
        settings = self._get_json(
            "https://dataminer2.pjm.com/config/settings.json",
        )

        return settings["subscriptionKey"]


"""
import gridstatus
iso = gridstatus.PJM()
nodes = iso.get_pnode_ids()
zones = nodes[nodes["pnode_subtype"] == "ZONE"]
zone_ids = zones["pnode_id"].tolist()
iso.get_historical_lmp("Oct 1, 2022", "DAY_AHEAD_HOURLY", locations=zone_ids)
pnode_id
"""


if __name__ == "__main__":

    import gridstatus

    iso = gridstatus.PJM()

    # df = iso.get_historical_fuel_mix(start="1/1/2016", end="10/16/2022")
    df = iso.get_historical_fuel_mix(start="11/6/2016")
