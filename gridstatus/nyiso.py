import io
from zipfile import ZipFile

import pandas as pd
import requests

import gridstatus
from gridstatus import utils
from gridstatus.base import FuelMix, GridStatus, ISOBase, Markets
from gridstatus.decorators import support_date_range

ZONE = "zone"
GENERATOR = "generator"
"""NYISO offers LMP data at two locational granularities: load zone and point of generator interconnection"""


class NYISO(ISOBase):
    """New York Independent System Operator (NYISO)"""

    name = "New York ISO"
    iso_id = "nyiso"
    default_timezone = "US/Eastern"
    markets = [Markets.REAL_TIME_5_MIN, Markets.DAY_AHEAD_HOURLY]
    status_homepage = "https://www.nyiso.com/system-conditions"

    @support_date_range(frequency="MS")
    def get_status(self, date, end=None, verbose=False):

        if date == "latest":
            latest = self._latest_from_today(self.get_status)
            return GridStatus(
                time=latest["time"],
                status=latest["status"],
                reserves=None,
                iso=self,
                notes=latest["notes"],
            )

        status_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name="RealTimeEvents",
        )

        status_df = status_df.rename(
            columns={"Message": "Status"},
        )

        def _parse_status(row):
            STATE_CHANGE = "**State Change. System now operating in "

            row["Notes"] = None
            if row["Status"] == "Start of day system state is NORMAL":
                row["Notes"] = [row["Status"]]
                row["Status"] = "Normal"
            elif STATE_CHANGE in row["Status"]:
                row["Notes"] = [row["Status"]]

                row["Status"] = row["Status"][
                    row["Status"].index(STATE_CHANGE)
                    + len(STATE_CHANGE) : -len(" state.**")
                ].capitalize()

            return row

        status_df = status_df.apply(_parse_status, axis=1)
        status_df = status_df[["Time", "Status", "Notes"]]
        return status_df

    @support_date_range(frequency="MS")
    def get_fuel_mix(self, date, end=None, verbose=False):
        # note: this is simlar datastructure to pjm

        if date == "latest":
            url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current"
            data = self._get_json(url)
            mix_df = pd.DataFrame(data["data"])
            time_str = mix_df["timeStamp"].max()
            time = pd.Timestamp(time_str)
            mix_df = mix_df[mix_df["timeStamp"] == time_str].set_index("fuelCategory")[
                "genMWh"
            ]
            mix_dict = mix_df.to_dict()
            return FuelMix(time=time, mix=mix_dict, iso=self.name)

        mix_df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name="rtfuelmix",
        )

        mix_df = mix_df.pivot_table(
            index="Time",
            columns="Fuel Category",
            values="Gen MW",
            aggfunc="first",
        ).reset_index()

        return mix_df

    @support_date_range(frequency="MS")
    def get_load(self, date, end=None, verbose=False):
        """Returns load at a previous date in 5 minute intervals"""
        if date == "latest":
            return self._latest_from_today(self.get_load)

        data = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name="pal",
        )

        # drop NA loads
        data = data.dropna(subset=["Load"])

        # TODO load by zone
        load = data.groupby("Time")["Load"].sum().reset_index()

        return load

    @support_date_range(frequency="MS")
    def get_supply(self, date, end=None, verbose=False):
        """Get supply for a date or date range in hourly intervals"""
        return self._get_supply(date=date, end=end, verbose=verbose)

    @support_date_range(frequency="MS")
    def get_load_forecast(self, date, end=None, verbose=False):
        """Get load forecast for a date in 1 hour intervals"""
        date = utils._handle_date(date, self.default_timezone)

        # todo optimize this to accept a date range
        data = self._download_nyiso_archive(
            date,
            end=end,
            dataset_name="isolf",
        )

        data = data[["File Date", "Time", "NYISO"]].rename(
            columns={
                "File Date": "Forecast Time",
                "NYISO": "Load Forecast",
                "Time": "Time",
            },
        )

        return data

    @support_date_range(frequency="MS")
    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = None,
        location_type: str = None,
    ):
        """
        Supported Markets: REAL_TIME_5_MIN, DAY_AHEAD_HOURLY

        Supported Location Types: "zone", "generator"
        """
        if date == "latest":
            return self._latest_lmp_from_today(
                market=market,
                locations=locations,
                location_type=location_type,
            )

        if locations is None:
            locations = "ALL"

        if location_type is None:
            location_type = ZONE

        assert market is not None, "market must be specified"
        market = Markets(market)
        marketname = self._set_marketname(market)
        location_type = self._set_location_type(location_type)
        filename = marketname + f"_{location_type}"

        df = self._download_nyiso_archive(
            date=date,
            end=end,
            dataset_name=marketname,
            filename=filename,
        )

        columns = {
            "Name": "Location",
            "LBMP ($/MWHr)": "LMP",
            "Marginal Cost Losses ($/MWHr)": "Loss",
            "Marginal Cost Congestion ($/MWHr)": "Congestion",
        }

        df = df.rename(columns=columns)

        df["Energy"] = df["LMP"] - (df["Loss"] - df["Congestion"])
        df["Market"] = market.value
        df["Location Type"] = "Zone" if location_type == ZONE else "Generator"

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

        df = utils.filter_lmp_locations(df, locations)

        return df

    def get_generators(self, verbose=False):
        """Get a list of generators in NYISO

        When possible return capacity information and fuel type information

        Parameters:
            verbose (bool): print out requested url

        Returns:
            pd.DataFrame: a dataframe of generators and locations
        """

        url = "http://mis.nyiso.com/public/csv/generator/generator.csv"

        if verbose:
            print(f"Requesting {url}")

        df = pd.read_csv(url)

        # need to be updated
        capacity_url_2022 = "https://www.nyiso.com/documents/20142/30338270/2022-NYCA-Generators.xlsx/f0526021-37fd-2c27-94ee-14d0f31878c1"

        if verbose:
            print(f"Requesting {url}")
        generators = pd.read_excel(
            capacity_url_2022,
            sheet_name=[
                "Table III-2a",
                "Table III-2b",
            ],
            skiprows=3,
            header=[0, 1, 2, 3, 4],
        )

        generators["Table III-2a"]["Generator Type"] = "Market Generator"
        generators["Table III-2b"]["Generator Type"] = "Non-Market Generator"

        # combined both sheets
        generators = pd.concat(generators.values())

        # manually transcribed column names
        generators.columns = [
            "LINE REF. NO.",
            "Owner, Operator, and / or Billing Organization",
            "Station Unit",
            "Zone",
            "PTID",
            "Town",
            "County",
            "State",
            "In-Service Date",
            "Name Plate Rating (V) MW",
            "2022 CRIS (A) MW Summer",
            "2022 CRIS MW Winter",
            "2022 Capability MW Summer",
            "2022 Capability MW Winter",
            "Is Dual Fuel",
            "Unit Type",
            "Fuel Type 1",
            "Fuel Type 2",
            "2021 Net Energy GWh",
            "Notes",
            "Generator Type",
        ]
        generators = generators.dropna(subset=["PTID"])

        generators["PTID"] = generators["PTID"].astype(int)

        # in other data
        generators = generators.drop(columns=["Zone"])

        combined = pd.merge(df, generators, on=["PTID"], how="left")

        unit_type_map = {
            "CC": "Combined Cycle",
            "CG": "Cogeneration",
            "CT": "Combustion Turbine Portion (CC)",
            "CW": "Waste Heat Only (CC)",
            "ES": "Energy Storage",
            "FC": "Fuel Cell",
            "GT": "Combustion Turbine",
            "HY": "Conventional Hydro",
            "IC": "Internal Combustion",
            "JE": "Jet Engine",
            "NB": "Steam (BWR Nuclear)",
            "NP": "Steam (PWR Nuclear)",
            "PS": "Pumped Storage Hydro",
            "PV": "Photovoltaic",
            "ST": "Steam Turbine (Fossil)",
            "WT": "Wind Turbine",
        }
        combined["Unit Type"] = combined["Unit Type"].map(unit_type_map)

        fuel_type_map = {
            "BAT": "Battery",
            "BUT": "Butane",
            "FO2": "No. 2 Fuel Oil",
            "FO4": "No. 4 Fuel Oil",
            "FO6": "No. 6 Fuel Oil",
            "FW": "Fly Wheel",
            "JF": "Jet Fuel",
            "KER": "Kerosene",
            "MTE": "Methane (Bio Gas)",
            "NG": "Natural Gas",
            "OT": "Other (Describe In Footnote)",
            "REF": "Refuse (Solid Waste)",
            "SUN": "Sunlight",
            "UR": "Uranium",
            "WAT": "Water",
            "WD": "Wood and/or Wood Waste",
            "WND": "Wind",
        }
        combined["Fuel Type 1"] = combined["Fuel Type 1"].map(
            fuel_type_map,
        )
        combined["Fuel Type 2"] = combined["Fuel Type 2"].map(
            fuel_type_map,
        )

        combined["Is Dual Fuel"] = combined["Is Dual Fuel"] == "YES"

        state_code_map = {
            36: "New York",
            42: "Pennsylvania",
            25: "Massachusetts",
            34: "New Jersey",
        }
        combined["State"] = combined["State"].map(state_code_map)

        # todo map county codes to names

        return combined

    def get_loads(self, verbose=False):
        """Get a list of loads in NYISO

        Parameters:
            verbose (bool): print out requested url

        Returns:
            pd.DataFrame: a dataframe of loads and locations
        """

        url = "http://mis.nyiso.com/public/csv/load/load.csv"

        if verbose:
            print(f"Requesting {url}")

        df = pd.read_csv(url)

        return df

    def _set_marketname(self, market: Markets) -> str:
        if market == Markets.REAL_TIME_5_MIN:
            marketname = "realtime"
        elif market == Markets.DAY_AHEAD_HOURLY:
            marketname = "damlbmp"
        else:
            raise RuntimeError("LMP Market is not supported")
        return marketname

    def _set_location_type(self, location_type: str) -> str:
        location_types = [ZONE, GENERATOR]
        if location_type == ZONE:
            return ZONE
        elif location_type == GENERATOR:
            return "gen"
        else:
            raise ValueError(
                f"Invalid location type. Expected one of: {location_types}",
            )

    def _download_nyiso_archive(self, date, end=None, dataset_name=None, filename=None):

        if filename is None:
            filename = dataset_name

        date = gridstatus.utils._handle_date(date)
        month = date.strftime("%Y%m01")
        day = date.strftime("%Y%m%d")

        csv_filename = f"{day}{filename}.csv"
        csv_url = f"http://mis.nyiso.com/public/csv/{dataset_name}/{csv_filename}"
        zip_url = (
            f"http://mis.nyiso.com/public/csv/{dataset_name}/{month}{filename}_csv.zip"
        )

        # the last 7 days of file are hosted directly as csv
        if end is None and date > pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize() - pd.DateOffset(days=7):
            df = pd.read_csv(csv_url)
            df = _handle_time(df)
            df["File Date"] = date.normalize()
        else:
            r = requests.get(zip_url)
            z = ZipFile(io.BytesIO(r.content))

            all_dfs = []
            if end is None:
                date_range = [date]
            else:
                try:
                    date_range = pd.date_range(
                        date,
                        end,
                        freq="1D",
                        inclusive="left",
                    )
                except TypeError:
                    date_range = pd.date_range(
                        date,
                        end,
                        freq="1D",
                        closed="left",
                    )

            for d in date_range:
                d = gridstatus.utils._handle_date(d)
                month = d.strftime("%Y%m01")
                day = d.strftime("%Y%m%d")

                csv_filename = f"{day}{filename}.csv"
                df = pd.read_csv(z.open(csv_filename))
                df["File Date"] = d.normalize()

                df = _handle_time(df)
                all_dfs.append(df)

            df = pd.concat(all_dfs)

        return df


def _handle_time(df):
    if "Time Stamp" in df.columns:
        time_stamp_col = "Time Stamp"
    elif "Timestamp" in df.columns:
        time_stamp_col = "Timestamp"

    def time_to_datetime(s, dst="infer"):
        return pd.to_datetime(s).dt.tz_localize(
            NYISO.default_timezone,
            ambiguous=dst,
        )

    if "Time Zone" in df.columns:
        dst = df["Time Zone"] == "EDT"
        df[time_stamp_col] = time_to_datetime(
            df[time_stamp_col],
            dst,
        )

    elif "Name" in df.columns:
        # once we group by name, the time series for each group is no longer ambiguous
        df[time_stamp_col] = df.groupby("Name", group_keys=False)[time_stamp_col].apply(
            time_to_datetime,
            "infer",
        )
    else:
        df[time_stamp_col] = time_to_datetime(
            df[time_stamp_col],
            "infer",
        )

    df = df.rename(columns={time_stamp_col: "Time"})

    return df


"""
pricing data

https://www.nyiso.com/en/energy-market-operational-data
"""
