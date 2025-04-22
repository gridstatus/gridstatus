import functools
import os
import pprint

import pandas as pd
import tqdm

from gridstatus import utils
from gridstatus.base import Markets


def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


# make custom function rather than using pd.date_range
# due to handling of localized timezones
def date_range_maker(start, end, freq, inclusive="neither"):
    """Generate a date range based on start and end dates and a frequency."""
    # implement other behavior
    # if/when needed
    assert inclusive == "neither"

    if isinstance(freq, str):
        freq = pd.tseries.frequencies.to_offset(freq)

    # Generate the date range
    current_date = start + freq

    dates = []
    while current_date < end:
        dates.append(current_date)
        current_date += freq

    return dates


# TODO(kladar): Add support for date or start to be in args OR kwargs dict as well, since some APIs have
# current or latest endpoints that are automatically handled. Currently cannot refactor this confidently
# without improved testing since it touches many methods
class support_date_range:
    def __init__(self, frequency, update_dates=None, return_raw=False):
        """Maximum frequency of ranges. if None, then no new ranges are created."""
        self.frequency = frequency
        self.update_dates = update_dates
        self.return_raw = return_raw

    def __call__(self, f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            args_dict = _get_args_dict(f, args, kwargs)

            # delete end if None to avoid attribute error
            if "end" in args_dict and not args_dict["end"]:
                del args_dict["end"]

            save_to = None
            if "save_to" in args_dict:
                save_to = args_dict.pop("save_to")
                os.makedirs(save_to, exist_ok=True)

            error = "ignore"
            errors = []
            if "error" in args_dict:
                error = args_dict.pop("error")

            # if date is a tuple, then change to start and end
            if "date" in args_dict and isinstance(args_dict["date"], tuple):
                args_dict["start"] = args_dict["date"][0]
                args_dict["end"] = args_dict["date"][1]
                del args_dict["date"]

            if "date" in args_dict and "start" in args_dict:
                raise ValueError(
                    "Cannot supply both 'date' and 'start' to function {}".format(
                        f,
                    ),
                )

            if "date" not in args_dict and "start" not in args_dict:
                raise ValueError(
                    "Must supply either 'date' or 'start' to function {}".format(
                        f,
                    ),
                )

            if "start" in args_dict:
                args_dict["date"] = args_dict["start"]
                del args_dict["start"]

            if args_dict["date"] == "latest":
                return f(*args, **kwargs)

            default_timezone = args_dict["self"].default_timezone

            # For today with sub daily data, create a range that spans the day
            if (
                self.frequency in ["HOUR_START", "5_MIN"]
                and args_dict.get("date") == "today"
            ):  # noqa
                args_dict["date"] = pd.Timestamp.now(tz=default_timezone).floor("D")
                args_dict["end"] = args_dict["date"] + pd.Timedelta(days=1)

            args_dict["date"] = utils._handle_date(
                args_dict["date"],
                default_timezone,
            )

            # no date range handling required
            if "end" not in args_dict:
                df = f(**args_dict)
                _handle_save_to(df, save_to, args_dict, f)
                return df

            if (
                isinstance(args_dict["end"], str)
                and args_dict["end"].lower() == "today"
            ):
                # add one day since end is exclusive
                args_dict["end"] = pd.Timestamp.now(
                    tz=default_timezone,
                ).date() + pd.DateOffset(days=1)

            args_dict["end"] = utils._handle_date(
                args_dict["end"],
                default_timezone,
            )

            assert args_dict["end"] > args_dict["date"], (
                "End date {} must be after start date {}".format(
                    args_dict["end"],
                    args_dict["date"],
                )
            )

            # if frequency is callable, then use it to get the frequency
            frequency = self.frequency
            if callable(frequency):
                frequency = self.frequency(args_dict)

            if frequency is None:
                dates = [args_dict["date"], args_dict["end"]]
            else:
                # Note: this may create a split that will end up
                # being unnecessary after running update dates below.
                # that is because after adding new dates, it's possible that two
                # ranges could be added.
                # Unnecessary optimization right now to include
                # logic to handle this
                # if certain frequency, we need to handle first interval
                # specially so pd.date_range works
                if frequency == "DAY_START":
                    frequency = DayBeginOffset()

                elif frequency == "MONTH_START":
                    frequency = MonthBeginOffset()

                elif frequency == "HOUR_START":
                    frequency = HourBeginOffset()

                elif frequency == "5_MIN":
                    frequency = FiveMinOffset()

                elif frequency == "YEAR_START":
                    frequency = YearBeginOffset()

                dates = date_range_maker(
                    args_dict["date"],
                    args_dict["end"],
                    freq=frequency,
                    inclusive="neither",
                )
                dates = [args_dict["date"]] + dates + [args_dict["end"]]

            # make sure everything is in default timezone
            # of the ISO
            dates = [utils._handle_date(d, default_timezone) for d in dates]

            # sometime api have restrictions/optimizations based on date ranges
            # update_dates allows for the caller to insert this logic
            if self.update_dates is not None:
                dates = self.update_dates(dates, args_dict)

            start_date = dates[0]

            # remove end date and add back later if needed
            del args_dict["end"]

            all_df = []

            # every None removes two possible queries
            total = len(dates) - dates.count(None) * 2 - 1

            with tqdm.tqdm(disable=total <= 1, total=total) as pbar:
                for end_date in dates[1:]:
                    # if we come across None, it means we should reset
                    if end_date is None:
                        start_date = None
                        continue

                    # if start_date is None, we just reset and end is actually the start
                    if start_date is None:
                        start_date = end_date
                        continue

                    args_dict["date"] = start_date

                    # no need for end if we are querying for just 1 day
                    if frequency != "1D" and not isinstance(frequency, DayBeginOffset):
                        args_dict["end"] = end_date

                    try:
                        df = f(**args_dict)
                    except Exception as e:
                        if error == "raise":
                            raise e
                        elif error == "ignore":
                            df = None
                            errors += [args_dict.copy()]
                            print("Error: {}".format(e))
                            print("Args: {}\n".format(args_dict))
                        else:
                            raise ValueError(
                                "Invalid value for error: {}".format(
                                    error,
                                ),
                            )

                    _handle_save_to(df, save_to, args_dict, f)

                    pbar.update(1)

                    if df is not None:
                        all_df.append(df)

                    start_date = end_date

            if errors:
                print("Errors that occurred while getting data:")
                pprint.pprint(errors)

            if self.return_raw:
                return all_df

            # if first item is a dict, then we need to concat by key
            if all_df and isinstance(all_df[0], dict):
                df = {}
                for d in all_df:
                    for k, v in d.items():
                        if k not in df:
                            df[k] = []
                        df[k].append(v)
                for k, v in df.items():
                    df[k] = pd.concat(v).reset_index(drop=True)
            else:
                df = pd.concat(all_df).reset_index(drop=True)

            return df

        return wrapped_f


def _handle_save_to(df, save_to, args_dict, f):
    if df is not None and save_to is not None:
        if "end" in args_dict:
            filename = "{}_{}_{}_{}.csv".format(
                args_dict["self"].__class__.__name__,
                f.__name__,
                args_dict["date"].strftime("%Y%m%d"),
                args_dict["end"].strftime("%Y%m%d"),
            )
        else:
            filename = "{}_{}_{}.csv".format(
                args_dict["self"].__class__.__name__,
                f.__name__,
                args_dict["date"].strftime("%Y%m%d"),
            )

        path = os.path.join(save_to, filename)

        df.to_csv(path, index=None)


def _get_pjm_archive_date(market):
    import gridstatus

    market = Markets(market)
    tz = gridstatus.PJM.default_timezone
    if market == Markets.REAL_TIME_5_MIN:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=186)
    elif market == Markets.REAL_TIME_HOURLY:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=731)
        # todo implemlement location type filter
    elif market == Markets.DAY_AHEAD_HOURLY:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=731)

    return archive_date.replace(hour=0, minute=0, second=0, microsecond=0)


# todo convert to custom PJMDateOffset class
def pjm_update_dates(dates, args_dict):
    """PJM has a weird API. This method updates the date range list to account
    for the following restrictions:

     - date ranges cannot span year boundaries
     - date ranges cannot span archive / standard boundaries
     - date range is inclusive of start and end dates
    """

    archive_date = _get_pjm_archive_date(args_dict["market"])

    new_dates = []

    for i, date in enumerate(dates):
        # stop if last date
        if i + 1 == len(dates):
            # add last date if new range has started
            if new_dates[-1] is not None:
                new_dates.append(date)

            break

        new_dates.append(date)

        # restriction 1: year boundary
        next_date = dates[i + 1]
        for year in range(date.year, next_date.year):
            current_year_end = pd.Timestamp(
                year=year,
                month=12,
                day=31,
                hour=23,
                minute=59,
                tz=args_dict["self"].default_timezone,
            )
            new_dates.append(current_year_end)
            next_year_start = pd.Timestamp(
                year=year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                tz=args_dict["self"].default_timezone,
            )

            new_dates.append(None)  # signal to skip to next date

            # dont need another range if the range ends at the start of the next year
            if next_year_start != next_date:
                new_dates.append(next_year_start)

    # remove trailing None
    if new_dates[-1] is None:
        new_dates = new_dates[:-1]

    # restriction 2: archive / standard boundary
    for i, date in enumerate(new_dates[:-1]):
        next_date = new_dates[i + 1]
        # check if archive date is between date and next_date
        if None not in [date, next_date] and date < archive_date < next_date:
            day_before_archive = archive_date - pd.Timedelta(days=1)
            add_before = pd.Timestamp(
                year=day_before_archive.year,
                month=day_before_archive.month,
                day=day_before_archive.day,
                hour=23,
                minute=59,
                tz=args_dict["self"].default_timezone,
            )

            new_dates = (
                new_dates[: i + 1]
                + [
                    add_before,
                    None,
                    archive_date,
                ]
                + new_dates[i + 1 :]
            )

    return new_dates


# custom offset that I dont believe exists in pandas
class DayBeginOffset:
    def __ladd__(self, other):
        return other.normalize() + pd.DateOffset(days=1)

    def __radd__(self, other):
        return self.__ladd__(other)


class MonthBeginOffset:
    def __ladd__(self, other):
        return other.normalize() + pd.offsets.MonthBegin(1)

    def __radd__(self, other):
        return self.__ladd__(other)


class FiveMinOffset:
    def __ladd__(self, other):
        # Store the original timezone
        original_tz = other.tz

        # Convert to UTC to avoid DST issues
        other_utc = other.tz_convert("UTC")

        # Round up to the next 5 min interval
        # Add 1 microsecond to ensure we make it to the
        # next interval when already on a 5 min interval
        rounded_utc = (other_utc + pd.Timedelta(microseconds=1)).ceil("5min")

        # Convert back to the original timezone
        s = rounded_utc.tz_convert(original_tz)

        return s

    def __radd__(self, other):
        return self.__ladd__(other)


class HourBeginOffset:
    def __ladd__(self, other):
        # Store the original timezone
        original_tz = other.tz

        # Convert to UTC to avoid DST issues
        other_utc = other.tz_convert("UTC")

        # Round up to the next hour
        # Add 1 microsecond to ensure we make it to the
        # next interval when already on a 5 min interval
        rounded_utc = (other_utc + pd.Timedelta(microseconds=1)).ceil("h")

        # Convert back to the original timezone
        s = rounded_utc.tz_convert(original_tz)

        return s

    def __radd__(self, other):
        return self.__ladd__(other)


class YearBeginOffset:
    def __ladd__(self, other):
        return other.normalize() + pd.offsets.YearBegin(1)

    def __radd__(self, other):
        return self.__ladd__(other)
