import requests
from .base import ISOBase, FuelMix
import pandas as pd
import isodata
import io
from zipfile import ZipFile


class NYISO(ISOBase):
    name = "New York ISO"
    iso_id = "nyiso"
    default_timezone = "US/Eastern"

    # def get_latest_status(self):
    #     # https://www.nyiso.com/en/system-conditions
    #      http://mis.nyiso.com/public/P-35list.htm
    #     pass

    def get_latest_fuel_mix(self):
        # note: this is simlar datastructure to pjm
        url = "https://www.nyiso.com/o/oasis-rest/oasis/currentfuel/line-current"
        data = self._get_json(url)
        mix_df = pd.DataFrame(data["data"])
        time_str = mix_df["timeStamp"].max()
        time = pd.Timestamp(time_str)
        mix_df = mix_df[mix_df["timeStamp"]
                        == time_str].set_index("fuelCategory")["genMWh"]
        mix_dict = mix_df.to_dict()
        return FuelMix(time=time, mix=mix_dict, iso=self.name)

    def get_fuel_mix_today(self):
        "Get fuel mix for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_fuel_mix)

    def get_fuel_mix_yesterday(self):
        "Get fuel mix for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_fuel_mix)

    def get_historical_fuel_mix(self, date):
        date = isodata.utils._handle_date(date)
        month = date.strftime('%Y%m01')
        day = date.strftime('%Y%m%d')

        file = f"{day}rtfuelmix.csv"
        csv_url = f'http://mis.nyiso.com/public/csv/rtfuelmix/{file}'
        zip_url = f'http://mis.nyiso.com/public/csv/rtfuelmix/{month}rtfuelmix_csv.zip'

        # the last 7 days of file are hosted directly as csv
        try:
            mix_df = pd.read_csv(csv_url)
        except:
            r = requests.get(zip_url)
            z = ZipFile(io.BytesIO(r.content))
            mix_df = pd.read_csv(z.open(file))

        mix_df = mix_df.pivot_table(index="Time Stamp",
                                    columns="Fuel Category", values="Gen MW", aggfunc="first").reset_index()

        mix_df["Time Stamp"] = pd.to_datetime(
            mix_df["Time Stamp"]).dt.tz_localize(self.default_timezone)

        mix_df = mix_df.rename(columns={"Time Stamp": "Time"})

        return mix_df

    def get_latest_demand(self):
        return self._latest_from_today(self.get_demand_today)

    def get_demand_today(self):
        "Get demand for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_demand)

    def get_demand_yesterday(self):
        "Get demand for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_demand)

    def get_historical_demand(self, date):
        date = isodata.utils._handle_date(date)

        url = "http://dss.nyiso.com/dss_oasis/PublicReports"
        data = {
            'reportKey': 'RT_ACT_LOAD',
            'startDate': date.strftime('%m/%d/%Y'),
            'endDate': date.strftime('%m/%d/%Y'),
            'version': 'L',  # latest versions
            'dataFormat': 'CSV',
            'filter': ['CAPITL', 'CENTRL', 'DUNWOD', 'GENESE', 'H Q', 'HUD VL', 'LONGIL', 'MHK VL', 'MILLWD', 'N.Y.C.', 'NORTH', 'NPX', 'O H', 'PJM', 'WEST']
        }
        r = requests.post(url, data=data)

        data = pd.read_csv(io.StringIO(r.content.decode("utf8")))
        demand = data.groupby("RTD End Time Stamp")[
            "RTD Actual Load"].sum().reset_index()

        demand = demand.rename(columns={"RTD End Time Stamp": "Time",
                                        "RTD Actual Load": "Demand"})

        demand["Time"] = pd.to_datetime(
            demand["Time"]).dt.tz_localize(self.default_timezone)

        return demand

    def get_latest_supply(self):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        mix = self.get_latest_fuel_mix()

        return {
            "time": mix.time,
            "supply": mix.total_production
        }

    def get_supply_today(self):
        "Get supply for today in 5 minute intervals"
        return self._today_from_historical(self.get_historical_supply)

    def get_supply_yesterday(self):
        "Get supply for yesterdat in 5 minute intervals"
        return self._yesterday_from_historical(self.get_historical_supply)

    def get_historical_supply(self, date):
        """Returns supply at a previous date in 5 minute intervals"""
        return self._supply_from_fuel_mix(date)


"""
pricing data

https://www.nyiso.com/en/energy-market-operational-data
"""
